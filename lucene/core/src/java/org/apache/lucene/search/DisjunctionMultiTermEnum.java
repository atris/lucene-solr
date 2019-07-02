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

import org.apache.lucene.index.FilteredTermsEnum;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;

/**
 * TermsEnum for DisjunctionMultiTermQuery
 */
public class DisjunctionMultiTermEnum extends FilteredTermsEnum {
  private final TermsEnum termsEnum;
  private final BytesRefHash filter;

  public DisjunctionMultiTermEnum(TermsEnum termsEnum, BytesRefHash filter) {
    super(termsEnum);
    this.termsEnum = termsEnum;
    this.filter = filter;
  }

  @Override
  protected AcceptStatus accept(BytesRef term) {
    return AcceptStatus.YES_AND_SEEK;
  }

  @Override
  public BytesRef next() throws IOException {
    BytesRef currentTerm = termsEnum.next();

    // Ideally this should be done during iterator construction, but that introduces
    // a lot more code just to manage the fact that the underlying TermsEnum is
    // exhausted before we start retrieving values from this enum
    while (currentTerm != null) {
      if (filter.find(currentTerm) != -1) {
        return currentTerm;
      }
      currentTerm = termsEnum.next();
    }

    return null;
  }
}
