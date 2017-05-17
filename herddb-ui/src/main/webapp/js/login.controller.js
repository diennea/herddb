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
    $scope.datasource = $location.search().url || 'jdbc:herddb:server:localhost';
    $scope.username = 'sa';
    $scope.pwd = 'hdb';
    $scope.defaultts = 'default';
    
    $scope.go = function (path, defaultts) {
        $location.path(path);
    };
    $scope.loginFromKeyUp = function (event) {
        if (event.keycode == '13') {
            $scope.login();
        }
    };
    $scope.login = function () {
        var url = getApplicationPath() + "/login";
        $.ajax({url: url,
            type: 'POST',
            data: {datasource: $scope.datasource,
                username: $scope.username,
                password: $scope.pwd,
                defaultts: $scope.defaultts},
            async: false,
            success: function (result) {
                if (result.ok) {
                    $.notifyClose();
                    $sharedTablespace.default = result.defaultts;
                    $sharedTablespace.jdbcurl = $scope.datasource;
                    $scope.go('/home', result.defaultts);
                } else {
                    if (!result.errormessage) {
                        showErrorNotify('Error on login', result.sqlerror);
                    } else {
                        showErrorNotify(result.errormessage, result.sqlerror);
                    }
                }
            }, error: function (result) {
                showErrorNotify('Error on retrieving data from url ' +url );
            }});
    };
    
    $(document).ready(function () {
        $('.wrapper').fadeIn(200);
    });
});
