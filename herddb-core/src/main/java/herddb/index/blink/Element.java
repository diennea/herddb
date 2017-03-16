/*
 Licensed to Diennea S.r.l. under one
 or more contributor license agreements. See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership. Diennea S.r.l. licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.

 */
package herddb.index.blink;

/**
 * @author diego.salvi
 */
final class Element<K> {
//  private static final class Element<K> {
//  private K key;
//  private long page;

  final K key;
  final long page;
//  private Element<K> next;
  Element<K> next;

  public Element(K key, long page) {
      this(key,page,null);
  }

  public Element(K key, long page, Element<K> next) {
      super();
      this.key = key;
      this.page = page;
      this.next = next;
  }

  @Override
  public String toString() {
      return "Element [key=" + key + ", page=" + page + "]";
  }
}