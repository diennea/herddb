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

modulo.controller('tablespaceController', function ($rootScope, $scope, $http, $route, $timeout, $location, $sharedTablespace) {
    console.log('initializing')
    $scope.actualPosition = 'tablespaces';
    $scope.lastAdvancedTablespaceTable = '';

    $scope.defaultTablespace = $sharedTablespace.actual;

    $scope.$watch('defaultTablespace', function (newValue, oldValue) {
        $sharedTablespace.actual = $scope.defaultTablespace;
    });

    $scope.$on('$includeContentLoaded', function (event) {
        selectActiveLiById('tablespaces-li');
    });


    $scope.go = function (path) {
        $location.path(path);
    };
    $scope.goToTableSpace = function (path) {
        onClickTablespace(path);
    };
    $scope.goToTable = function (path) {
        onClickTablespace(path);
    };
    $scope.refresh = function (forceValue, first) {
        if (!$scope.defaultTablespace || first) {
            $scope.defaultTablespace = $sharedTablespace.default;
            console.log('1');
        }
        if (!$scope.defaultTablespace) {
            console.log('2');
            $http.get('http://localhost:8086/herddb-ui/webresources/api/defaultts').success(function (data) {
                console.log('3');
                $scope.defaultTablespace = data.default;
                $sharedTablespace.actual = $scope.defaultTablespace;
                doRefreshThings(forceValue);
            });
        } else {
            console.log('4');
            doRefreshThings(forceValue);
        }
    }


    function doRefreshThings(forceValue) {
        console.log('dorefthinfs' + $scope.actualTableSpace);
        var value = forceValue ? forceValue : $scope.actualPosition;
        console.log('val' + value);
        if (value == 'tablespaces') {
            onClickTablespaces();
        } else if (value == 'tablespace') {
            onClickTablespace($scope.actualTableSpace);
            if ($scope.lastAdvancedTablespaceTable == 'transactions') {
                $scope.showTransactions();
            } else if ($scope.lastAdvancedTablespaceTable == 'stats') {
                $scope.showStats();
            } else if ($scope.lastAdvancedTablespaceTable == 'tables') {
                $scope.showTables();
            }
        } else if (value == 'table') {
            onClickTable($scope.actualTable);
        }
    }

    $scope.requestTableSpaces = function () {
        var url = "http://localhost:8086/herddb-ui/webresources/api/tablespaces?defaultts=" + encodeURIComponent($scope.defaultTablespace);
        $http.get(url).
                success(function (data, status, headers, config) {
                    var $table = $('#table-tablespaces');
                    if ($.fn.dataTable.isDataTable($table)) {
                        $table.empty();
                        $table.DataTable().destroy();
                    }
                    var opt = getCommonDatableOptions();
                    opt.data = data;
                    opt.columns = [{title: 'name'}, {title: 'UUID'}, {title: 'Leader'}, {title: 'Replica'}, {title: 'expectedreplicacount'}, {title: 'maxleaderinactivitytime'}];
                    var table = $table.DataTable(opt);

                    $('#table-tablespaces').find('td').click(function () {
                        var clicked = $('#table-tablespaces').DataTable().row(this).data()[0];
                        onClickTablespace(clicked);
                    });
                }).
                error(function (data, status, headers, config) {
                    console.error('error');
                    showErrorNotify('Error on retrieving data from ' + url);
                });
    };

    $scope.showTables = function () {
        $scope.lastAdvancedTablespaceTable = 'tables';
        $('.div-tablespace-hiddendata').hide();
        var url = "http://localhost:8086/herddb-ui/webresources/api/tablespace/tables?ts=" + encodeURIComponent($scope.actualTableSpace);
        $http.get(url).
                success(function (data, status, headers, config) {

                    var $table_tables = $('#table-tablespace-tables');
                    if ($.fn.dataTable.isDataTable($table_tables)) {
                        $table_tables.empty();
                        $table_tables.DataTable().destroy();
                    }
                    var opt_tables = getCommonDatableOptions();
                    opt_tables['data'] = data.tables;
                    opt_tables['columns'] = [{title: 'Table name'}];
                    $('#div-tablespace-tables').fadeIn(100);
                    $table_tables.DataTable(opt_tables);

                    $table_tables.find('td').click(function () {
                        var clicked = $table_tables.DataTable().row(this).data()[0];
                        onClickTable(clicked);
                    });



                }).
                error(function (data, status, headers, config) {
                    console.error('error');
                    showErrorNotify('Error on retrieving data from ' + url);
                });
    }
    $scope.showTransactions = function (a) {
        console.log(a);
        $scope.lastAdvancedTablespaceTable = 'transactions';
        $('.div-tablespace-hiddendata').hide();
        var url = "http://localhost:8086/herddb-ui/webresources/api/tablespace/transactions?ts=" + encodeURIComponent($scope.actualTableSpace);
        $http.get(url).
                success(function (data, status, headers, config) {

                    var $table = $('#table-tablespace-transactions');
                    if ($.fn.dataTable.isDataTable($table)) {
                        $table.empty();
                        $table.DataTable().destroy();
                    }
                    var opt = getCommonDatableOptions();
                    opt['data'] = data.transactions;
                    opt.columns = [{title: 'Transaction ID'}, {title: 'Creation date'}];
                    $('#div-tablespace-transactions').fadeIn(300);
                    $table.DataTable(opt);

                }).
                error(function (data, status, headers, config) {
                    console.error('error');
                    showErrorNotify('Error on retrieving data from ' + url);
                });
    }
    $scope.showStats = function () {
        console.log('from stat' + $scope.actualTableSpace);
        console.log($scope);
        $scope.lastAdvancedTablespaceTable = 'stats';
        $('.div-tablespace-hiddendata').hide();
        var url = "http://localhost:8086/herddb-ui/webresources/api/tablespace/stats?ts=" + encodeURIComponent($scope.actualTableSpace);
        $http.get(url).
                success(function (data, status, headers, config) {
                    var $table_stats = $('#table-tablespace-stats');
                    if ($.fn.dataTable.isDataTable($table_stats)) {
                        $table_stats.empty();
                        $table_stats.DataTable().destroy();
                    }
                    var opt_stats = getCommonDatableOptions();
                    opt_stats['data'] = data.stats;
                    opt_stats.columns = [{title: 'Table Name'},
                        {title: 'System Table'},
                        {title: 'Replica'},
                        {title: 'TableSize'},
                        {title: 'Loaded Pages'},
                        {title: 'Unloaded Pages'},
                        {title: 'Dirty Pages'},
                        {title: 'Dirty Records'},
                        {title: 'Max logical page size'},
                        {title: 'Keys memory'},
                        {title: 'Buffers memory'},
                        {title: 'Dirty Memory'}, ];
                    $('#div-tablespace-stats').fadeIn(300);

                    $table_stats.DataTable(opt_stats);

                }).
                error(function (data, status, headers, config) {
                    console.error('error');
                    showErrorNotify('Error on retrieving data from ' + url);
                });
    }

    $scope.requestTableSpace = function (ts) {
        $('.div-tablespace-hiddendata').hide();
        var url = "http://localhost:8086/herddb-ui/webresources/api/tablespace/replicastate?ts=" + encodeURIComponent(ts);
        $scope.actualTableSpace = ts;
        console.log($scope.actualTableSpace)
        $http.get(url).
                success(function (data, status, headers, config) {
                    var $table_repl = $('#table-tablespace-replication');
                    if ($.fn.dataTable.isDataTable($table_repl)) {
                        $table_repl.empty();
                        $table_repl.DataTable().destroy();
                    }
                    var opt_repl = getCommonDatableOptions();
                    opt_repl['data'] = data.replication;
                    opt_repl.columns = [{title: 'UUID'},
                        {title: 'Node ID'},
                        {title: 'Mode'},
                        {title: 'Timestamp'},
                        {title: 'Max leader inactivity time'},
                        {title: 'Inactivity time'}];
                    $table_repl.DataTable(opt_repl);

                }).
                error(function (data, status, headers, config) {
                    console.error('error');
                    showErrorNotify('Error on retrieving data from ' + url);
                });
    };
    $scope.requestTable = function (table) {
        var url = "http://localhost:8086/herddb-ui/webresources/api/table?tb=" + encodeURIComponent(table) + "&ts=" + encodeURIComponent($scope.actualTableSpace);
        $scope.actualTable = table;
        $http.get(url).
                success(function (data, status, headers, config) {
                    var $table = $('#table-table-metadata');
                    if ($.fn.dataTable.isDataTable($table)) {
                        $table.empty();
                        $table.DataTable().destroy();
                    }
                    var opt = getCommonDatableOptions();
                    opt['data'] = data.metadata;
                    opt.columns = [{title: 'Column name'},
                        {title: 'Ordinal position'},
                        {title: 'Nullable'},
                        {title: 'Data type'},
                        {title: 'Auto increment'}];
                    $table.DataTable(opt);

                    var $table_data = $('#table-table-indexes');
                    if ($.fn.dataTable.isDataTable($table_data)) {
                        $table_data.empty();
                        $table_data.DataTable().destroy();
                    }
                    var opt = getCommonDatableOptions();
                    opt['data'] = data.indexes;
                    opt.columns = [{title: 'Index name'},
                        {title: 'Index type'}];

                    $table_data.DataTable(opt);

                }).
                error(function (data, status, headers, config) {
                    console.error('error');
                    showErrorNotify('Error on retrieving data from ' + url);
                });
    };

    function onClickTable(table) {
        addBread(table, 'table-crumb');
        $scope.requestTable(table);
        $('#div-tablespaces').hide();
        $('#div-tablespace').hide();
        $('#div-table').fadeIn(500);
        $scope.actualPosition = 'table';
    }
    function onClickTablespace(ts) {
        removeBread('table-crumb');
        addBread(ts, 'tablespace-crumb');
        $scope.requestTableSpace(ts);
        $('#div-tablespaces').hide();
        $('#div-table').hide();
        $('#div-tablespace').fadeIn(500);
        $scope.actualPosition = 'tablespace';
    }
    function onClickTablespaces() {
        removeAllBreads();
        $scope.requestTableSpaces();
        $('#div-tablespace').hide();
        $('#div-table').hide();
        $('#div-tablespaces').fadeIn(500);
        $scope.actualPosition = 'tablespaces';
    }

    function addBread(text, clazz) {
        if ($('.' + clazz).length == 0) {
            var span = $('<span />').addClass('ti-arrow-circle-right navbar-brand crumb-right-arrow').addClass(clazz);
            var crumb = $('<a />').addClass('navbar-brand crumbs').addClass(clazz).click(function () {
                if (crumb.hasClass('tablespace-crumb')) {
                    onClickTablespace(text);
                } else if (crumb.hasClass('table-crumb')) {
                    onClickTable(text);
                }

            }).text(text);
            $('#breadcrumb').append(span).append(crumb);
        }
    }
    function removeAllBreads() {
        $('#breadcrumb').children(':not(.maincrumb)').remove();
    }
    function removeBread(clazz) {
        $('#breadcrumb').children('.' + clazz).remove();
    }
    $scope.checkLogin = function () {
        $http.get('http://localhost:8086/herddb-ui/webresources/api/checklogin').success(function (data) {
            console.log(data);
            if (data == false) {
                $scope.go('/login');
                return false;
            } else {
                $scope.refresh('tablespaces', true);
            }
        }).error(function () {
            console.log('errot on check login');
            $scope.go('/login');
            return false;
        });
    }

    $(document).ready(function () {
        $('#div-all').fadeIn(200);
        $scope.checkLogin();
    });
});


