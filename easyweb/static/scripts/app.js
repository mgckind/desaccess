(function(document) {
  'use strict';

  var app = document.querySelector('#app');

  app.baseUrl = '/';
  if (window.location.port === '') {  // if production
  };


  window.addEventListener('WebComponentsReady', function() {
    var pages = document.getElementById("mainPages");
    var menu = document.querySelector('paper-menu');
    var appr = document.getElementById('approute');
    // var tabs = document.getElementById('help-tabs');
    //
    // var prevButton = document.getElementById("prev");
    // var nextButton = document.getElementById("next");
    // var cid = "home-crs";
    //
    // if (appr.data.view == '') sel_page = "home-crs";
    // if (appr.data.view == 'db-access') sel_page = "query-crs";
    // if (appr.data.view == 'db-schema') sel_page = "all-crs";
    // if (appr.data.view == 'db-examples') sel_page = "example-crs";
    // if (appr.data.view == 'cutouts') sel_page = "coadd-crs";
    // if (appr.data.view == 'footprint') sel_page = "footprint-crs";
    // if (appr.data.view == 'my-jobs') sel_page = "jobs-crs";
    // if (appr.data.view == 'help-form') sel_page = "help-crs";

    var sel_page = "0";
    if (appr.data.view == 'db-access') sel_page = "1";
    if (appr.data.view == 'db-schema') sel_page = "2";
    if (appr.data.view == 'db-examples') sel_page = "3";
    if (appr.data.view == 'cutouts') sel_page = "4";
    if (appr.data.view == 'footprint') sel_page = "5";
    if (appr.data.view == 'my-jobs') sel_page = "6";
    if (appr.data.view == 'help-form') sel_page = "7";
    app.selection=sel_page;
    pages.select(sel_page);
    menu.select(sel_page);

      // tabs.select(sel_page);

      // pages.selected="0";
    // menu.selected="0";
    appr.addEventListener('data-changed', function(event) {
        event.stopPropagation();
       if (appr.data.view == '') sel_page = "0";
    if (appr.data.view == 'db-access') sel_page = "1";
    if (appr.data.view == 'db-schema') sel_page = "2";
    if (appr.data.view == 'db-examples') sel_page = "3";
    if (appr.data.view == 'cutouts') sel_page = "4";
    if (appr.data.view == 'footprint') sel_page = "5";
    if (appr.data.view == 'my-jobs') sel_page = "6";
    if (appr.data.view == 'help-form') sel_page = "7";
        app.selection=sel_page;
        pages.select(sel_page);
        menu.select(sel_page);

        // tabs.select(sel_page);
    });
        menu.addEventListener('iron-select', function() {
            app.selection=menu.selected;
            // pages.selected=menu.selected;
            pages.select(menu.selected)
            app.editor.refresh();
            if (app.$.drawerLayout.narrow) {app.$.drawer.close();}
        });
        menu.addEventListener('iron-activate', function() {
           //app.$.drawer.close();
        });

        myQuery = document.getElementById("queryBox");
        app.editor = CodeMirror.fromTextArea(myQuery, {
            lineNumbers: true,
            mode: 'text/x-plsql',
            autofocus: true,
        });
        app.editor.setValue('-- Insert Query --\n');
        app.editor.focus();
        app.editor.execCommand('goLineDown');
        myJobQuery = document.getElementById("jobQueryBox");
        app.jobquerybox = CodeMirror.fromTextArea(myJobQuery, {
            lineNumbers: false,
            mode: 'text/x-plsql',
            readOnly: true,
            autofocus: true,
        });
        app.jobquerybox.setValue('\n\n\n\n\n\n\n\n\n\n');
        app.jobquerybox.focus();
        myExampleQuery = document.getElementById("exampleQueryBox");
        app.examplequerybox = CodeMirror.fromTextArea(myExampleQuery, {
            lineNumbers: false,
            mode: 'text/x-plsql',
            readOnly: true,
            autofocus: true,
            viewportMargin: 50,
        });
        app.examplequerybox.setValue('\n\n\n\n\n\n\n\n\n\n\n');
        app.examplequerybox.focus();

        var xsize = document.getElementById("xsizeSlider");
               xsize.addEventListener('value-change', function() {
                   document.getElementById("xsizeLabel").textContent = xsize.value;
               });
               var ysize = document.getElementById("ysizeSlider");
               ysize.addEventListener('value-change', function() {
                   document.getElementById("ysizeLabel").textContent = ysize.value;
               });
      //
      // prevButton.addEventListener('focused-changed', function(){
      //     console.log("=> page: ", cid);
      //     var crs = document.getElementById(cid);
      //     crs.goToPrevPage();
      // });
      //
      // nextButton.addEventListener('focused-changed', function(){
      //     console.log("=> page: ", cid);
      //     var crs = document.getElementById(cid);
      //     crs.goToPrevPage();
      // });



  });



})(document);
