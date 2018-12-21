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
package herddb.utils;

/**
 * Utility
 *
 * @author enrico.olivelli
 * @author diego.salvi
 */
public class SQLUtils {

    public static String escape(String s) {
        if (s == null) {
            return s;
        }
        return s.replace("'", "''");
    }
    
    /** {@link #findQueryStart(String)}: reading empty data */
    private static final int FIND_START_STATE_NORMAL = 0;
    
    /** {@link #findQueryStart(String)}: reading inline comment */
    private static final int FIND_START_STATE_SINGLE_LINE_COMMENT = 5;
    
    /** {@link #findQueryStart(String)}: opening inline comment */
    private static final int FIND_START_STATE_SINGLE_LINE_COMMENT_IN = 6;
    
    /** {@link #findQueryStart(String)}: reading multiline comment */
    private static final int FIND_START_STATE_MULTILINE_COMMENT = 1;
    
    /** {@link #findQueryStart(String)}: opening multiline comment */
    private static final int FIND_START_STATE_MULTILINE_COMMENT_IN = 2;
    
    /** {@link #findQueryStart(String)}: closing multiline comment */
    private static final int FIND_START_STATE_MULTILINE_COMMENT_OUT = 3;
    
    /**
     * Search where the real SQL query starts skipping <i>empty</i> prefixes:
     * <ul>
     * <li>whitespaces</li>
     * <li>tabs</li>
     * <li>carriage returns</li>
     * <li>new lines</li>
     * <li>multi line comments: &#47;&#42; comment &#42;&#47;</li>
     * <li>single line comments: -- comment</li>
     * </ul>
     * 
     * @param query
     * 
     * @return index of the first not <i>empty</i> character or -1 if the query
     *         contains only <i>empty</i> characters
     */
    public static final int findQueryStart(String query)
    {
        final int max = query.length();
        int idx = 0;
        
        int state = FIND_START_STATE_NORMAL;
        
        while( idx < max )
        {
            char ch = query.charAt(idx);
            
            switch (state)
            {
                case FIND_START_STATE_SINGLE_LINE_COMMENT_IN:
                    
                    switch( ch )
                    {
                        case '-':
                            state = FIND_START_STATE_SINGLE_LINE_COMMENT;
                            break;
                        
                        default:
                            
                            /* Back to previous character */
                            return idx--;
    
                    }
                    
                    break;
                    
                case FIND_START_STATE_SINGLE_LINE_COMMENT:
                    
                    if ( ch == '\n' )
                        state = FIND_START_STATE_NORMAL;
                    
                    break;
                
                case FIND_START_STATE_MULTILINE_COMMENT_IN:
                    
                    switch( ch )
                    {
                        case '*':
                            state = FIND_START_STATE_MULTILINE_COMMENT;
                            break;
                        
                        default:
                            
                            /* Back to previous character */
                            return idx--;
                    }
                    
                    break;
                
                case FIND_START_STATE_MULTILINE_COMMENT:
                    
                    if ( ch == '*' )
                        state = FIND_START_STATE_MULTILINE_COMMENT_OUT;
                    
                    break;
                    
                case FIND_START_STATE_MULTILINE_COMMENT_OUT:
                    
                    if ( ch == '/' )
                        state = FIND_START_STATE_NORMAL;
                    else
                        state = FIND_START_STATE_MULTILINE_COMMENT;
                    
                    break;
                    
                case FIND_START_STATE_NORMAL:
                    
                    switch (ch)
                    {
                        case '-':
                            state = FIND_START_STATE_SINGLE_LINE_COMMENT_IN;
                            break;
                        
                        case '/':
                            state = FIND_START_STATE_MULTILINE_COMMENT_IN;
                            break;
    
                        case '\n':
                        case '\r':
                        case '\t':
                        case ' ':
                            state = FIND_START_STATE_NORMAL;
                            break;
                            
                        default:
                            
                            return idx;
                    }
                    
                    break;
                    
            }
            
            ++idx;
            
        }
        
        /* No match at all. Only ignorable charaters and comments */
        return -1;
    }
    
}
