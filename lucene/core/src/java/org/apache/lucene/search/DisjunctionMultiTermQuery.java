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

import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRefHash;

/**
 * A MultiTermQuery implementation allowing multiple terms
 * to be ORed together. This class will short circuit the first
 * time a match is seen.
 */
public class DisjunctionMultiTermQuery extends MultiTermQuery {

  private DisjunctionMultiTermEnum disjunctionMultiTermEnum;
  private final BytesRefHash inputSet = new BytesRefHash();

  public DisjunctionMultiTermQuery(String field, Term... terms) {
    super(field);
    assert terms != null;

    for (Term term : terms) {
      inputSet.add(term.bytes());
    }
  }

  @Override
  public void visit(QueryVisitor visitor) {
    visitor.visitLeaf(this);
  }

  @Override
  protected TermsEnum getTermsEnum(Terms terms, AttributeSource atts) throws IOException {
    TermsEnum termsEnum = terms.iterator();

    disjunctionMultiTermEnum = new DisjunctionMultiTermEnum(termsEnum, inputSet);

    return disjunctionMultiTermEnum;
  }

  @Override
  public String toString(String field) {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("DisjunctionMultiTermQuery");

    stringBuilder.setLength(stringBuilder.length() - 1);
    stringBuilder.append("]");
    return stringBuilder.toString();
  }
}
