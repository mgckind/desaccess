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

    var sel_page = "des-home";
    if (appr.data.view == 'db-access') sel_page = "des-query";
    if (appr.data.view == 'db-schema') sel_page = "des-schema";
    if (appr.data.view == 'db-examples') sel_page = "des-example-query";
    //if (appr.data.view == 'cutouts') sel_page = "4";
    if (appr.data.view == 'bulk-cutouts') sel_page = "des-bulk-cutouts";
    if (appr.data.view == 'deslabs') sel_page = "des-jlabs";
    if (appr.data.view == 'finding-chart') sel_page = "des-finding-chart";
    if (appr.data.view == 'footprint') sel_page = "des-footprint";
     if (appr.data.view == 'data-analysis') sel_page = "des-data-analysis";
    if (appr.data.view == 'my-jobs') sel_page = "des-my-jobs";
    if (appr.data.view == 'help-form') sel_page = "des-help";
    app.selection=sel_page;
    pages.select(sel_page);
    menu.select(sel_page);

      // tabs.select(sel_page);

      // pages.selected="0";
    // menu.selected="0";
    appr.addEventListener('data-changed', function(event) {
        event.stopPropagation();
    var sel_page = "des-home";
    if (appr.data.view == 'db-access') sel_page = "des-query";
    if (appr.data.view == 'db-schema') sel_page = "des-schema";
    if (appr.data.view == 'db-examples') sel_page = "des-example-query";
    //if (appr.data.view == 'cutouts') sel_page = "4";
    if (appr.data.view == 'bulk-cutouts') sel_page = "des-bulk-cutouts";
    if (appr.data.view == 'deslabs') sel_page = "des-jlabs";
    if (appr.data.view == 'finding-chart') sel_page = "des-finding-chart";
    if (appr.data.view == 'footprint') sel_page = "des-footprint";
     if (appr.data.view == 'data-analysis') sel_page = "des-data-analysis";
    if (appr.data.view == 'my-jobs') sel_page = "des-my-jobs";
    if (appr.data.view == 'help-form') sel_page = "des-help";
        app.selection=sel_page;
        pages.select(sel_page);
        menu.select(sel_page);

        // tabs.select(sel_page);
    });
        menu.addEventListener('iron-select', function() {
            app.selection=menu.selected;
            // pages.selected=menu.selected;
            pages.select(menu.selected);
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
        app.editor.setValue('-- Insert Query --\n\n\n');
        app.editor.focus();
        app.editor.execCommand('goLineDown');
        myJobQuery = document.getElementById("jobQueryBox");
        app.jobquerybox = CodeMirror.fromTextArea(myJobQuery, {
            lineNumbers: false,
            mode: 'text/x-plsql',
            readOnly: 'nocursor',
            autofocus: true,
        });
        app.jobquerybox.setValue('\n\n\n\n\n\n\n\n\n\n');
        app.jobquerybox.focus();
        myExampleQuery = document.getElementById("exampleQueryBox");
        app.examplequerybox = CodeMirror.fromTextArea(myExampleQuery, {
            lineNumbers: false,
            mode: 'text/x-plsql',
            readOnly: 'nocursor',
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

        var xsizeS = document.getElementById("xsizeSliderS");
        xsizeS.addEventListener('value-change', function() {
           document.getElementById("xsizeLabelS").textContent = xsizeS.value;
        });
        var ysizeS = document.getElementById("ysizeSliderS");
        ysizeS.addEventListener('value-change', function() {
           document.getElementById("ysizeLabelS").textContent = ysizeS.value;
        });

        var fc_xsize = document.getElementById("fc_xsizeSlider");
        fc_xsize.addEventListener('value-change', function() {
            document.getElementById("fc_xsizeLabel").textContent = fc_xsize.value;
        });
        var fc_ysize = document.getElementById("fc_ysizeSlider");
        fc_ysize.addEventListener('value-change', function() {
            document.getElementById("fc_ysizeLabel").textContent = fc_ysize.value;
        });
        var fc_mag = document.getElementById("fc_magSlider");
		fc_mag.addEventListener('value-change', function() {
		  document.getElementById("fc_magLabel").textContent = fc_mag.value;
	    });
	    var da_radius = document.getElementById("da_radiusSlider");
        da_radius.addEventListener('value-change', function() {
            document.getElementById("da_radiusLabel").textContent = da_radius.value;
        });

        var bc_xsize = document.getElementById("bc_xsizeSlider");
        bc_xsize.addEventListener('value-change', function() {
           document.getElementById("bc_xsizeLabel").textContent = bc_xsize.value;
        });
        var bc_ysize = document.getElementById("bc_ysizeSlider");
        bc_ysize.addEventListener('value-change', function() {
           document.getElementById("bc_ysizeLabel").textContent = bc_ysize.value;
        });

      // var res = document.getElementById("response");
      // res.style.marginTop = $("#queryBox").height() + $("#query-table").height() + 35;
      // console.log("margin top of res: ", res.style.marginTop)
      // //
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
