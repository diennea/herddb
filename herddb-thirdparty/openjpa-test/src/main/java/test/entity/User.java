/*
 * Licensed to Diennea S.r.l. under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Diennea S.r.l. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package test.entity;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Lob;
import javax.persistence.OneToOne;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Entity
@NoArgsConstructor
@AllArgsConstructor
@SuppressFBWarnings(value = {"UWF_UNWRITTEN_FIELD", "NP_BOOLEAN_RETURN_NULL"},
        justification = " Unwritten field: test.entity.User.pcPCSuperclass [test.entity.User] "
        + " test.entity.User.pcIsDetached() has Boolean return type and returns explicit null")
public class User {  // this is a reserved word for Calcite

    @Id
    @GeneratedValue
    private long id;
    @Column(unique = true)
    private String name;
    // this is a reserved word for Calcite
    private int value;
    @Lob
    private String description;

    @OneToOne(cascade = CascadeType.ALL)
    private Address address;
}
