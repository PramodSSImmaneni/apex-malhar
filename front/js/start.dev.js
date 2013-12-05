/*
* Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*   http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
// Set the datatorrent library to DT, global object.
window.DT = require('./datatorrent');

// Register pre-compiled templates
var kt = require('knights-templar');
var precompiled = require('./precompiled-templates');
kt.registerPrecompiled(precompiled);

// Set up the options for the dashboard
var appOptions = {
    host: window.location.host,
    pages: require('./app/pages')
};

// Start the app in the #wrapper element on load
$(window).ready(function() {
    appOptions.el = document.getElementById('wrapper');
    var app = new DT.lib.App(appOptions);
});