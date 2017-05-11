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

var modulo = angular.module('herddb-ui-module', ['ngRoute']);

modulo.factory('$sharedTablespace', function () {
    var tablespace = {};
    tablespace.default;
    tablespace.actual;
    return tablespace;
});

modulo.config(function ($routeProvider) {
    $routeProvider.when('/home',
            {templateUrl: 'html/tablespaces.html', controller: 'tablespaceController'});
    $routeProvider.when('/login',
            {templateUrl: 'html/login.html', controller: 'loginController'});
    $routeProvider.when('/pag2',
            {templateUrl: 'html/nodes.html', controller: 'nodeController'});
    $routeProvider.otherwise({redirectTo: '/home'});
});

modulo.controller('logoutController', function ($http, $scope, $location) {
    $scope.logout = function () {
        $http.get('http://localhost:8086/herddb-ui/webresources/api/logout').success(function (data) {
            $location.path('/login');
        });
    };
});


function selectActiveLi(li) {
    $('li').removeClass('active');
    $(li).addClass('active');
}
function selectActiveLiById(id) {
    $('li').removeClass('active');
    $('#' + id).addClass('active');
}

function getCommonDatableOptions() {

    return {searching: false,
        scrollY: '100vh',
        scrollCollapse: true,
        paging: false,
        info: false,
    }
}
function logout() {
    $.ajax({url: "http://localhost:8086/herddb-ui/webresources/api/logout",
        type: 'GET',
        success: function (res) {
            return true;
        }, error: function (result) {
            return false;
        }});
}
function checkLogin() {
    $.ajax({url: "http://localhost:8086/herddb-ui/webresources/api/checklogin",
        type: 'GET',
        success: function (res) {
            if (res == "true")
                return true;
            if (res == "false")
                return false;
        }, error: function (result) {
            return false;
        }});
}
function showErrorNotify(msg) {
    $.notify({
        icon: "ti-na",
        message: msg

    }, {
        type: 'danger',
        delay: 2000,
        placement: {
            from: 'top',
            align: 'center'
        }
        , animate: {
            enter: 'animated fadeInDown',
            exit: 'animated fadeOutUp'
        },
    });
}