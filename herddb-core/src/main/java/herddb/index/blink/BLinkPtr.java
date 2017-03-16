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
final class BLinkPtr {

    public static final byte LINK  = -1;
    public static final byte PAGE  =  1;
    public static final byte EMPTY  = 0;

    private static final BLinkPtr EMPTY_INSTANCE = new BLinkPtr(BLink.NO_PAGE, EMPTY);

    long value;
    byte type;

    BLinkPtr(long value, byte type) {
        super();
        this.value = value;
        this.type = type;
    }

    public static final BLinkPtr link(long value) {
        return  (value == BLink.NO_PAGE) ? EMPTY_INSTANCE : new BLinkPtr(value, LINK);
    }

    public static final BLinkPtr page(long value) {
        return  (value == BLink.NO_PAGE) ? EMPTY_INSTANCE : new BLinkPtr(value, PAGE);
    }

    public static final BLinkPtr empty() {
        return EMPTY_INSTANCE;
    }

    public boolean isLink() {
        return type == LINK;
    }

    public boolean isPage() {
        return type == PAGE;
    }

    public boolean isEmpty() {
        return type == EMPTY;
    }

    @Override
    public String toString() {
        switch (type) {
            case LINK:
                return "BLinkPtr [value=" + value + ", type=LINK]";
            case PAGE:
                return "BLinkPtr [value=" + value + ", type=PAGE]";
            case EMPTY:
                return "BLinkPtr [value=" + value + ", type=EMPTY]";
            default:
                return "BLinkPtr [value=" + value + ", type=" + type + "]";
        }
    }

}
