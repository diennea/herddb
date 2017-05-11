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

modulo.controller('loginController', function ($scope, $http, $route, $timeout, $location, $sharedTablespace) {
    $scope.datasource = 'jdbc:herddb:server:localhost';
    $scope.username = 'sa';
    $scope.pwd = 'hdb';
    $scope.defaultts = 'default';
    
    $scope.go = function (path, defaultts) {
        $location.path(path);
    };
    $scope.login = function () {

        $.ajax({url: "http://localhost:8086/herddb-ui/webresources/api/login",
            type: 'POST',
            data: {datasource: $scope.datasource,
                username: $scope.username,
                password: $scope.pwd,
                defaultts: $scope.defaultts},
            async: false,
            success: function (result) {
                console.log(result)
                if (result.ok) {
                    $sharedTablespace.default = result.defaultts;
                    $scope.go('/home', result.defaultts);
                } else {
                    console.log('error on login')
                    showErrorNotify('The credentials are not correct');
                }
            }, error: function (result) {
                console.log('error on login')
                showErrorNotify('The credentials are not correct');
            }});
    }
    
    $(document).ready(function () {
    })
});
