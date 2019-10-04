/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MockRandomMergePolicy;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.NamedThreadFactory;
import org.apache.lucene.util.TestUtil;

public class TestTopFieldCollectorEarlyTermination extends LuceneTestCase {

  private int numDocs;
  private List<String> terms;
  private Directory dir;
  private final Sort sort = new Sort(new SortField("ndv1", SortField.Type.LONG));
  private RandomIndexWriter iw;
  private IndexReader reader;
  private static final int FORCE_MERGE_MAX_SEGMENT_COUNT = 5;

  private Document randomDocument() {
    final Document doc = new Document();
    doc.add(new NumericDocValuesField("ndv1", random().nextInt(10)));
    doc.add(new NumericDocValuesField("ndv2", random().nextInt(10)));
    doc.add(new StringField("s", RandomPicks.randomFrom(random(), terms), Store.YES));
    return doc;
  }

  private void createRandomIndex(boolean singleSortedSegment) throws IOException {
    dir = newDirectory();
    numDocs = atLeast(150);
    final int numTerms = TestUtil.nextInt(random(), 1, numDocs / 5);
    Set<String> randomTerms = new HashSet<>();
    while (randomTerms.size() < numTerms) {
      randomTerms.add(TestUtil.randomSimpleString(random()));
    }
    terms = new ArrayList<>(randomTerms);
    final long seed = random().nextLong();
    final IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(new Random(seed)));
    if (iwc.getMergePolicy() instanceof MockRandomMergePolicy) {
      // MockRandomMP randomly wraps the leaf readers which makes merging angry
      iwc.setMergePolicy(newTieredMergePolicy());
    }
    iwc.setMergeScheduler(new SerialMergeScheduler()); // for reproducible tests
    iwc.setIndexSort(sort);
    iw = new RandomIndexWriter(new Random(seed), dir, iwc);
    iw.setDoRandomForceMerge(false); // don't do this, it may happen anyway with MockRandomMP
    for (int i = 0; i < numDocs; ++i) {
      final Document doc = randomDocument();
      iw.addDocument(doc);
      if (i == numDocs / 2 || (i != numDocs - 1 && random().nextInt(8) == 0)) {
        iw.commit();
      }
      if (random().nextInt(15) == 0) {
        final String term = RandomPicks.randomFrom(random(), terms);
        iw.deleteDocuments(new Term("s", term));
      }
    }
    if (singleSortedSegment) {
      iw.forceMerge(1);
    }
    else if (random().nextBoolean()) {
      iw.forceMerge(FORCE_MERGE_MAX_SEGMENT_COUNT);
    }
    reader = iw.getReader();
    if (reader.numDocs() == 0) {
      iw.addDocument(new Document());
      reader.close();
      reader = iw.getReader();
    }
  }
  
  private void closeIndex() throws IOException {
    reader.close();
    iw.close();
    dir.close();
  }

  private IndexSearcher createConcurrentSearcherWithSmallSlices(ExecutorService executor) {
    return new IndexSearcher(reader, executor) {
      @Override
      protected LeafSlice[] slices(List<LeafReaderContext> leaves) {
        ArrayList<LeafSlice> slices = new ArrayList<>();
        for (LeafReaderContext ctx : leaves) {
          slices.add(new LeafSlice(Arrays.asList(ctx)));
        }

        return slices.toArray(new LeafSlice[0]);
      }
    };
  }

  public void testEarlyTermination() throws IOException {
    doTestEarlyTermination(false);
  }

  public void testEarlyTerminationWhenPaging() throws IOException {
    doTestEarlyTermination(true);
  }

  private void doTestEarlyTermination(boolean paging) throws IOException {
    final int iters = atLeast(8);
    for (int i = 0; i < iters; ++i) {
      createRandomIndex(false);
      int maxSegmentSize = 0;
      for (LeafReaderContext ctx : reader.leaves()) {
        maxSegmentSize = Math.max(ctx.reader().numDocs(), maxSegmentSize);
      }
      ExecutorService service = new ThreadPoolExecutor(4, 4, 0L, TimeUnit.MILLISECONDS,
          new LinkedBlockingQueue<Runnable>(),
          new NamedThreadFactory("TestIndexSearcher"));
      for (int j = 0; j < iters; ++j) {
        final IndexSearcher[] searchers = {newSearcher(reader), createConcurrentSearcherWithSmallSlices(service)};
        for (IndexSearcher searcher : searchers) {
          final int numHits = TestUtil.nextInt(random(), 1, numDocs);
          FieldDoc after;
          if (paging) {
            assert searcher.getIndexReader().numDocs() > 0;
            TopFieldDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
            after = (FieldDoc) td.scoreDocs[td.scoreDocs.length - 1];
          } else {
            after = null;
          }
          final TopFieldCollector collector1 = TopFieldCollector.create(sort, numHits, after, Integer.MAX_VALUE);
          final TopFieldCollector collector2 = TopFieldCollector.create(sort, numHits, after, 1);

          final Query query;
          if (random().nextBoolean()) {
            query = new TermQuery(new Term("s", RandomPicks.randomFrom(random(), terms)));
          } else {
            query = new MatchAllDocsQuery();
          }
          TopDocs td1;
          TopDocs td2;

          if (searcher.getExecutor() != null) {
            CollectorManager<TopFieldCollector, TopFieldDocs> collectorManager = TopFieldCollector.createSharedManager(sort,
                numHits, after, 1);
            td2 = searcher.search(query, collectorManager);
            searcher.search(query, collector1);
            td1 = collector1.topDocs();
          } else {
            searcher.search(query, collector1);
            searcher.search(query, collector2);
            td1 = collector1.topDocs();
            td2 = collector2.topDocs();
          }

          assertFalse(collector1.isEarlyTerminated());
          if (paging == false && maxSegmentSize > numHits && query instanceof MatchAllDocsQuery &&
              searcher.getExecutor() == null) {
            // Make sure that we sometimes early terminate
            assertTrue(collector2.isEarlyTerminated());
          }
          if (collector2.isEarlyTerminated()) {
            assertTrue(td2.totalHits.value >= td1.scoreDocs.length);
            assertTrue(td2.totalHits.value <= reader.maxDoc());
          } else {
            if (searcher.getExecutor() == null) {
              assertEquals(td2.totalHits.value, td1.totalHits.value);
            }
          }
          CheckHits.checkEqual(query, td1.scoreDocs, td2.scoreDocs);
        }
      }
      closeIndex();
      service.shutdown();
    }
  }
  
  public void testCanEarlyTerminateOnDocId() {
    assertTrue(TopFieldCollector.canEarlyTerminate(
        new Sort(SortField.FIELD_DOC),
        new Sort(SortField.FIELD_DOC)));
    
    assertTrue(TopFieldCollector.canEarlyTerminate(
        new Sort(SortField.FIELD_DOC),
        null));

    assertFalse(TopFieldCollector.canEarlyTerminate(
        new Sort(new SortField("a", SortField.Type.LONG)),
        null));

    assertFalse(TopFieldCollector.canEarlyTerminate(
        new Sort(new SortField("a", SortField.Type.LONG)),
        new Sort(new SortField("b", SortField.Type.LONG))));

    assertTrue(TopFieldCollector.canEarlyTerminate(
        new Sort(SortField.FIELD_DOC),
        new Sort(new SortField("b", SortField.Type.LONG))));

    assertTrue(TopFieldCollector.canEarlyTerminate(
        new Sort(SortField.FIELD_DOC),
        new Sort(new SortField("b", SortField.Type.LONG), SortField.FIELD_DOC)));

    assertFalse(TopFieldCollector.canEarlyTerminate(
        new Sort(new SortField("a", SortField.Type.LONG)),
        new Sort(SortField.FIELD_DOC)));

    assertFalse(TopFieldCollector.canEarlyTerminate(
        new Sort(new SortField("a", SortField.Type.LONG), SortField.FIELD_DOC),
        new Sort(SortField.FIELD_DOC)));
  }

  public void testCanEarlyTerminateOnPrefix() {
    assertTrue(TopFieldCollector.canEarlyTerminate(
        new Sort(new SortField("a", SortField.Type.LONG)),
        new Sort(new SortField("a", SortField.Type.LONG))));

    assertTrue(TopFieldCollector.canEarlyTerminate(
        new Sort(new SortField("a", SortField.Type.LONG), new SortField("b", SortField.Type.STRING)),
        new Sort(new SortField("a", SortField.Type.LONG), new SortField("b", SortField.Type.STRING))));

    assertTrue(TopFieldCollector.canEarlyTerminate(
        new Sort(new SortField("a", SortField.Type.LONG)),
        new Sort(new SortField("a", SortField.Type.LONG), new SortField("b", SortField.Type.STRING))));

    assertFalse(TopFieldCollector.canEarlyTerminate(
        new Sort(new SortField("a", SortField.Type.LONG, true)),
        null));
    
    assertFalse(TopFieldCollector.canEarlyTerminate(
        new Sort(new SortField("a", SortField.Type.LONG, true)),
        new Sort(new SortField("a", SortField.Type.LONG, false))));

    assertFalse(TopFieldCollector.canEarlyTerminate(
        new Sort(new SortField("a", SortField.Type.LONG), new SortField("b", SortField.Type.STRING)),
        new Sort(new SortField("a", SortField.Type.LONG))));

    assertFalse(TopFieldCollector.canEarlyTerminate(
        new Sort(new SortField("a", SortField.Type.LONG), new SortField("b", SortField.Type.STRING)),
        new Sort(new SortField("a", SortField.Type.LONG), new SortField("c", SortField.Type.STRING))));

    assertFalse(TopFieldCollector.canEarlyTerminate(
        new Sort(new SortField("a", SortField.Type.LONG), new SortField("b", SortField.Type.STRING)),
        new Sort(new SortField("c", SortField.Type.LONG), new SortField("b", SortField.Type.STRING))));
  }


  public void testGlobalEarlyTermination() throws IOException {
    Sort sort = new Sort(
        new SortField("value2", SortField.Type.LONG, false));

    doEarlyTerminationTestSingleFieldDocument(sort, false);
  }

  public void testGlobalEarlyTerminationReverse() throws IOException {
    Sort sort = new Sort(
        new SortField("value2", SortField.Type.LONG, true));

    doEarlyTerminationTestSingleFieldDocument(sort, true);
  }

  public void testGlobalEarlyTerminationMultiple() throws IOException {
    Sort sort = new Sort(
        new SortField("value1", SortField.Type.STRING, false),
        new SortField("value2", SortField.Type.LONG, false));

    doEarlyTerminationTestMultipleFieldsDocument(sort, false);
  }

  public void testGlobalEarlyTerminationMultipleReverse() throws IOException {
    Sort sort = new Sort(
        new SortField("value1", SortField.Type.STRING, true),
        new SortField("value2", SortField.Type.LONG, true));

    doEarlyTerminationTestMultipleFieldsDocument(sort, true);
  }

  private void doEarlyTerminationTestSingleFieldDocument(Sort sort, boolean reverse) throws IOException {
    Directory dir = newDirectory();

    final long seed = random().nextLong();
    final IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(new Random(seed)));
    if (iwc.getMergePolicy() instanceof MockRandomMergePolicy) {
      // MockRandomMP randomly wraps the leaf readers which makes merging angry
      iwc.setMergePolicy(newTieredMergePolicy());
    }
    iwc.setMergeScheduler(new SerialMergeScheduler()); // for reproducible tests
    iwc.setIndexSort(sort);

    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, iwc);
    int firstSegmentNumDocs = atLeast(10);
    int secondSegmentNumDocs = atLeast(5);

    for (int i = 0; i < firstSegmentNumDocs; i++) {
      Document doc = new Document();
      doc.add(new NumericDocValuesField("value2", 0));
      doc.add(newStringField("value2", "1", Field.Store.YES));
      writer.addDocument(doc);
    }
    writer.commit();

    for (int i = 0; i < secondSegmentNumDocs; i++) {
      Document doc = new Document();
      doc.add(new NumericDocValuesField("value2", 1));
      doc.add(newStringField("value2", "0", Field.Store.YES));
      writer.addDocument(doc);
    }
    writer.commit();

    reader = writer.getReader();
    writer.close();

    ExecutorService service = new ThreadPoolExecutor(4, 4, 0L, TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<Runnable>(),
        new NamedThreadFactory("TestTopFieldCollectorEarlyTermination"));

    IndexSearcher searcher = createConcurrentSearcherWithSmallSlices(service);

    final CollectorManager<TopFieldCollector, TopFieldDocs> manager = new CollectorManager<TopFieldCollector, TopFieldDocs>() {

      private final HitsThresholdChecker hitsThresholdChecker = HitsThresholdChecker.createShared(10);
      private final FieldValueChecker fieldValueChecker = FieldValueChecker.createFieldValueChecker(sort, 2);

      @Override
      public TopFieldCollector newCollector() throws IOException {
        return TopFieldCollector.create(sort, 2, null, hitsThresholdChecker, fieldValueChecker);
      }

      @Override
      public TopFieldDocs reduce(Collection<TopFieldCollector> collectors) throws IOException {
        final TopFieldDocs[] topDocs = new TopFieldDocs[collectors.size()];
        int i = 0;
        for (TopFieldCollector collector : collectors) {
          topDocs[i++] = collector.topDocs();
        }
        return TopDocs.merge(sort, 0, 2, topDocs);
      }

    };

    TopDocs td = searcher.search(new MatchAllDocsQuery(), manager);

    if (searcher.getSlices().length > 1) {
      assert td.totalHits.value < (firstSegmentNumDocs + secondSegmentNumDocs);
    }

    if (reverse) {
      assertEquals("0", searcher.doc(td.scoreDocs[0].doc).get("value2"));
      assertEquals("0", searcher.doc(td.scoreDocs[1].doc).get("value2"));
    } else {
      assertEquals("1", searcher.doc(td.scoreDocs[0].doc).get("value2"));
      assertEquals("1", searcher.doc(td.scoreDocs[1].doc).get("value2"));
    }

    service.shutdown();
    reader.close();
    dir.close();
  }

  private void doEarlyTerminationTestMultipleFieldsDocument(Sort sort, boolean reverse) throws IOException {
    Directory dir = newDirectory();

    final long seed = random().nextLong();
    final IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(new Random(seed)));
    if (iwc.getMergePolicy() instanceof MockRandomMergePolicy) {
      // MockRandomMP randomly wraps the leaf readers which makes merging angry
      iwc.setMergePolicy(newTieredMergePolicy());
    }
    iwc.setMergeScheduler(new SerialMergeScheduler()); // for reproducible tests
    iwc.setIndexSort(sort);

    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, iwc);
    int firstSegmentNumDocs = atLeast(10);
    int secondSegmentNumDocs = atLeast(5);

    for (int i = 0; i < firstSegmentNumDocs; i++) {
      Document doc = new Document();
      doc.add(new SortedDocValuesField("value1", new BytesRef("bar")));
      doc.add(new NumericDocValuesField("value2", 1));
      doc.add(newStringField("value1", "bar", Field.Store.YES));
      doc.add(newStringField("value2", "1", Field.Store.YES));
      writer.addDocument(doc);
    }
    writer.commit();

    for (int i = 0; i < secondSegmentNumDocs; i++) {
      Document doc = new Document();
      doc.add(new SortedDocValuesField("value1", new BytesRef("foo")));
      doc.add(new NumericDocValuesField("value2", 0));
      doc.add(newStringField("value1", "foo", Field.Store.YES));
      doc.add(newStringField("value2", "0", Field.Store.YES));
      writer.addDocument(doc);
    }
    writer.commit();

    reader = writer.getReader();
    writer.close();

    ExecutorService service = new ThreadPoolExecutor(4, 4, 0L, TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<Runnable>(),
        new NamedThreadFactory("TestTopFieldCollectorEarlyTermination"));

    IndexSearcher searcher = createConcurrentSearcherWithSmallSlices(service);

    final CollectorManager<TopFieldCollector, TopFieldDocs> manager = new CollectorManager<TopFieldCollector, TopFieldDocs>() {

      private final HitsThresholdChecker hitsThresholdChecker = HitsThresholdChecker.createShared(10);
      private final FieldValueChecker fieldValueChecker = FieldValueChecker.createFieldValueChecker(sort, 2);

      @Override
      public TopFieldCollector newCollector() throws IOException {
        return TopFieldCollector.create(sort, 2, null, hitsThresholdChecker, fieldValueChecker);
      }

      @Override
      public TopFieldDocs reduce(Collection<TopFieldCollector> collectors) throws IOException {
        final TopFieldDocs[] topDocs = new TopFieldDocs[collectors.size()];
        int i = 0;
        for (TopFieldCollector collector : collectors) {
          topDocs[i++] = collector.topDocs();
        }
        return TopDocs.merge(sort, 0, 2, topDocs);
      }

    };

    TopDocs td = searcher.search(new MatchAllDocsQuery(), manager);

    if (searcher.getSlices().length > 1) {
      assert td.totalHits.value < (firstSegmentNumDocs + secondSegmentNumDocs);
    }

    if (reverse) {
      assertEquals("foo", searcher.doc(td.scoreDocs[0].doc).get("value1"));
      assertEquals("foo", searcher.doc(td.scoreDocs[1].doc).get("value1"));
      assertEquals("0", searcher.doc(td.scoreDocs[0].doc).get("value2"));
      assertEquals("0", searcher.doc(td.scoreDocs[1].doc).get("value2"));
    } else {
      assertEquals("bar", searcher.doc(td.scoreDocs[0].doc).get("value1"));
      assertEquals("bar", searcher.doc(td.scoreDocs[1].doc).get("value1"));
      assertEquals("1", searcher.doc(td.scoreDocs[0].doc).get("value2"));
      assertEquals("1", searcher.doc(td.scoreDocs[1].doc).get("value2"));
    }

    service.shutdown();
    reader.close();
    dir.close();
  }
}
