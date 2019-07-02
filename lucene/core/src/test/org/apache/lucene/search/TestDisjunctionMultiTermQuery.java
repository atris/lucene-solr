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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

public class TestDisjunctionMultiTermQuery extends LuceneTestCase {

  private static String FIELD_T = "T";
  private static String FIELD_C = "C";

  private TermQuery t1 = new TermQuery(new Term(FIELD_T, "files"));
  private TermQuery t2 = new TermQuery(new Term(FIELD_T, "deleting"));
  private TermQuery t3 = new TermQuery(new Term(FIELD_T, "nonmatching"));

  private TermQuery c1 = new TermQuery(new Term(FIELD_C, "production"));
  private TermQuery c2 = new TermQuery(new Term(FIELD_C, "optimize"));

  private IndexSearcher searcher = null;
  private Directory dir;
  private IndexReader reader;

  @Override
  public void setUp() throws Exception {
    super.setUp();

    dir = newDirectory();

    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

    for (int i = 0; i < 50; i++) {
      Document d = new Document();
      d.add(newField(
          FIELD_T,
          "Optimize all files",
          TextField.TYPE_STORED));
      d.add(newField(
          FIELD_T,
          "deleting foo and bar",
          TextField.TYPE_STORED));

      d.add(newField(
          FIELD_C,
          "optimize in our production environment.",
          TextField.TYPE_STORED));

      //
      writer.addDocument(d);
    }

    for (int i = 0; i < 50; i++) {
      Document d = new Document();
      d.add(newField(
          FIELD_T,
          "files are open",
          TextField.TYPE_STORED));

      //
      writer.addDocument(d);
    }

    for (int i = 0; i < 50; i++) {
      Document d = new Document();
      d.add(newField(
          FIELD_T,
          "Optimize the cool stuff",
          TextField.TYPE_STORED));

      //
      writer.addDocument(d);
    }

    reader = writer.getReader();
    //
    searcher = newSearcher(reader);
    writer.close();
  }

  @Override
  public void tearDown() throws Exception {
    reader.close();
    dir.close();
    super.tearDown();
  }

  private long search(Query q) throws IOException {
    QueryUtils.check(random(), q,searcher);
    return searcher.search(q, 1000).totalHits.value;
  }

  public void testMultiTermDisjunction() throws IOException {
    DisjunctionMultiTermQuery q = new DisjunctionMultiTermQuery("T", new Term[]{t1.getTerm(), t2.getTerm(), c1.getTerm()});
    assertEquals(100, search(q));
  }

  public void testFoo() throws IOException {
    DisjunctionMultiTermQuery q = new DisjunctionMultiTermQuery("T", new Term[]{t2.getTerm(), t3.getTerm(), c2.getTerm()});
    assertEquals(100, search(q));
  }

  public void testBar() throws IOException {
    DisjunctionMultiTermQuery q = new DisjunctionMultiTermQuery("T", new Term[]{t1.getTerm(), t3.getTerm()});
    assertEquals(100, search(q));
  }
}
