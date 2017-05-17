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
    tablespace.jdbcurl;
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
        $http.get('webresources/api/logout').success(function (data) {
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

function checkLogin() {
    $.ajax({url: "webresources/api/checklogin",
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

function showErrorNotify(msg, sqlerror) {
    var message = '<span>' + msg + '</span>';
    if (sqlerror) {
        message = '<span>' + msg + "</span><br><button class='btn btn-danger btn-fill btn-xs' onclick='showDetails(this)'>Details</button><br><span id='show-details-span' style='display: none'>" + sqlerror + '<span>'
    }
    $.notify({
        icon: "ti-na",
        message: message

    }, {
        type: 'danger',
        mouse_over: 'pause',
        placement: {
            from: 'top',
            align: 'left'
        }

    });

}

function showDetails(a) {
    $(a).parent().find('#show-details-span').toggle();
}
function getApplicationPath() {
    return "webresources/api";
}
function startProgressBar() {
    $('.progress-span').show();
    var $progress = $('.progress-bar');
    $progress.css('width', '100%').attr('aria-valuenow', 100);

}
function stopProgressBar() {
    $('.progress-span').hide();
    $('.progress-bar').css('width', '0%').attr('aria-valuenow', 0);
}