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

package herddb.cli;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang.StringUtils;

/**
 * @author francesco.caliumi
 */
public class TextTableBuilder {
    private List<String[]> rows = new ArrayList<String[]>();
    private boolean hasResults = false;

    private void addRowInternal(List<String> values) {
        String[] vals = new String[values.size()];
        rows.add(values.toArray(vals));
    }

    public void addRow(List<String> values) {
        hasResults = true;
        addRowInternal(values);
    }

    public void addIntestation(List<String> columns) {
        addRowInternal(columns);
        List<String> dashes = new ArrayList<>();
        for (String c : columns) {
            dashes.add(StringUtils.repeat("-", c.length()));
        }
        addRowInternal(dashes);
    }

    private int[] colWidths() {
        int cols = -1;

        for (String[] row : rows) {
            cols = Math.max(cols, row.length);
        }

        int[] widths = new int[cols];

        for (String[] row : rows) {
            for (int colNum = 0; colNum < row.length; colNum++) {
                widths[colNum] =
                        Math.max(
                                widths[colNum],
                                StringUtils.length(row[colNum]));
            }
        }

        return widths;
    }

    @Override
    public String toString() {
        if (!hasResults) {
            return "Empty results set\n";
        }

        StringBuilder buf = new StringBuilder();

        int[] colWidths = colWidths();

        for (String[] row : rows) {
            buf.append("| ");
            for (int colNum = 0; colNum < row.length; colNum++) {
                buf.append(
                        StringUtils.rightPad(
                                StringUtils.defaultString(
                                        row[colNum]), colWidths[colNum]));
                buf.append(" | ");
            }

            buf.append('\n');
        }

        return buf.toString();
    }

}
