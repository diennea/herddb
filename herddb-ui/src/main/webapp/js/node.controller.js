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

modulo.controller('nodeController', function ($scope, $http, $route, $timeout, $location, $sharedTablespace) {
    $scope.defaultTablespace = $sharedTablespace.actual;
    $scope.go = function (path) {
        $location.path(path);
    };
    $scope.defaultTablespace = $sharedTablespace.actual;
    $scope.$watch('defaultTablespace', function (newValue, oldValue) {
        $sharedTablespace.actual = $scope.defaultTablespace;
    });

    $scope.$on('$includeContentLoaded', function (event) {
        selectActiveLiById('nodes-li');
    });

    $scope.$on('refresh', function (event, mass) {
        if (mass == 'nodes') {
            $scope.refresh('nodes');
        }
    });


    $scope.refresh = function (forceValue) {
        if (!$scope.JDBCURL) {
            $http.get(getApplicationPath() + '/jdbcurl').success(function (data) {
                $scope.JDBCURL = data.url;
            });
        }
        if (!$scope.defaultTablespace) {
            $scope.defaultTablespace = $sharedTablespace.default;
        }
        if (!$scope.defaultTablespace) {
            $http.get(getApplicationPath() + '/defaultts').success(function (data) {
                $scope.defaultTablespace = data.default;
                $sharedTablespace.actual = $scope.defaultTablespace;
                doRefreshThings(forceValue);
            });
        } else {
            doRefreshThings(forceValue)
        }
    }
    $scope.refreshFromKeyUp = function (e) {
        if (e.keyCode == '13') {
            $scope.refresh();
        }
    };


    function doRefreshThings(forceValue) {
        var value = forceValue ? forceValue : $scope.actualPosition;
        if (value == 'nodes') {
            onClickNodes();
        } else if (value == 'node') {
            onClickNode($scope.actualNode);
        }
    }


    $scope.requestNodes = function () {
        var url = "http://localhost:8086/herddb-ui/webresources/api/nodes?defaultts=" + encodeURIComponent($scope.defaultTablespace);
        var $table = $('#table-nodes');
        if ($.fn.dataTable.isDataTable($table)) {
            $table.empty();
            $table.DataTable().destroy();
        }
        $http.get(url).
                success(function (data, status, headers, config) {

                    var opt = getCommonDatableOptions();
                    opt['data'] = data;
                    opt.columns = [{title: 'Node id'}, {title: 'Address'}, {title: 'SSL'}]
                    var table = $table.DataTable(opt);
                    stopProgressBar();
                    $table.find('td').click(function () {
                        var clicked = $table.DataTable().row(this).data()[0];
                        onClickNode(clicked);
                    });
                }).
                error(function (data, status, headers, config) {
                    showErrorNotify('Error on retrieving data from ' + url);
                });
    };


    $scope.requestNode = function (node) {
        $scope.actualNode = node;
        var url = "http://localhost:8086/herddb-ui/webresources/api/node?nd=" + encodeURIComponent(node) + "&defaultts=" + encodeURIComponent($scope.defaultTablespace);
        var $table = $('#table-node');
        if ($.fn.dataTable.isDataTable($table)) {
            $table.empty();
            $table.DataTable().destroy();
        }
        $http.get(url).
                success(function (data, status, headers, config) {

                    var opt = getCommonDatableOptions();
                    opt['data'] = data.data;
                    opt.columns = [{title: "key"}, {title: 'value'}];
                    var table = $table.DataTable(opt);
                    stopProgressBar();

                }).
                error(function (data, status, headers, config) {
                    showErrorNotify('Error on retrieving data from ' + url);
                });
    };

    function onClickNodes() {
        startProgressBar();
        removeAllCrumbs();
        $scope.actualPosition = 'nodes';
        $scope.requestNodes();
        $('#div-node').hide();
        $('#div-nodes').fadeIn(300);
    }
    function onClickNode(node) {
        startProgressBar();
        addCrumb(node, 'node');
        $scope.actualPosition = 'node';
        $scope.requestNode(node);
        $('#div-nodes').hide();
        $('#div-node').fadeIn(300);
    }

    $scope.checkLogin = function () {
        $http.get(getApplicationPath() + '/checklogin').success(function (data) {
            if (data == false) {
                $scope.go('/login');
            } else {
                $('#div-all').fadeIn(200);
                $scope.refresh('nodes');
            }
        }).error(function () {
            $scope.go('/login');
        });
    }

    $(document).ready(function () {
        $scope.JDBCURL = $sharedTablespace.jdbcurl;
        $scope.checkLogin();
    });

    function addCrumb(text, clazz) {
        if ($('.' + clazz).length == 0) {
            $('.maincrumb, .crumbs').removeClass('active');
            var span = $('<span />').addClass('ti-arrow-circle-right navbar-brand crumb-right-arrow').addClass(clazz);
            var crumb = $('<a />').addClass('navbar-brand crumbs active').addClass(clazz).click(function () {
                $('.maincrumb, .crumbs').removeClass('active');
                crumb.addClass('active');
                onClickNode(text)
            }).text(text);
            $('#breadcrumb').append(span).append(crumb);
        }
    }
    function removeAllCrumbs() {
        $('#breadcrumb').children(':not(.maincrumb)').remove();
        $('.maincrumb').addClass('active');
    }
});
