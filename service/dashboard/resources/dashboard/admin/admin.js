var limit;

$(function(){
	$('#admin')
		.on('click', '.infoLine a', function(e){
			e.stopPropagation();
		})
		.on('click', '.infoLine', function(){
			var self = this;
			if($(self).is('a')) return;
			$('.details', $(self).parents('.instance')).slideToggle();
		})
		.on('click', '.details h3', function(){
			var self = this;
			$(self).next().fadeToggle();
		});

	$('#expand').toggle(
		function(){
	    $('#sidebar').animate({width:'0px'},300);
	    $('#admin').animate({'margin-right':'0px'},300);
	    $('#expand').text("show nav");},
		function(){
	    $('#sidebar').animate({width:'300px'},300);
	    $('#admin').animate({'margin-right':'300px'},300);
			$('#expand').text("fullscreen");
		});

	$('#sidebar').on('click', 'a', function(){
		var self = this,
				chartURL = $(self).attr('href'),
				chartName = $(self).attr('data-type');

		$('#admin').html("Loading...")
		if(chartName == 'services') {
			loadServices();
		} else if (chartName == 'graphs') {
			var self = this;
			limit = $(self).attr('data-param');
			$.getJSON('/admin/metric-graphs?limit=' + limit + '&des-pts=300', function(data){
				var ks = Object.keys(data).sort();
				$('#admin').empty();
        for (i in ks) {
            k = ks[i]
            $('#admin').append($('<h2>').html(k));
            appendTimeseries("#admin", data[k], 600, 300, limit);
        }
			});
		} else if (chartName == 'machines') {
			var self = this;
			limit = $(self).attr('data-param');
			$.getJSON('/admin/machine-graphs?limit=' + limit + '&des-pts=300', function(data){
				var ks = Object.keys(data).sort();
				$('#admin').empty();
        for (i in ks) {
            k = ks[i]
            $('#admin').append($('<h2>').html(k));
            appendTimeseries("#admin", data[k], 600, 300, limit);
        }
			});
		} else if (chartName == 'table') {
			var self = this,
					link = $(self).attr('href');
			$.getJSON(link, function(data){
				$('#admin').html(tableHTML(data));
			});
		} else if (chartName == 'topic-table') {
			var self = this,
				link = $(self).attr('href');
			$.getJSON(link, function(data){
				$('#admin').html('');
				var topicsheader = $('<div>').attr('id', 'topicsheader').appendTo('#admin'),
						topics = $('<div>').attr('id', 'topics').appendTo('#admin');
	      for(var k in data) {
	        var name = data[k][0]
	        var body = data[k][1] 
	        $(topicsheader).append("<span id='topicheader" + k + "' class='topicheader' onclick='activate("+k+")'>" + name + " </span>&nbsp;&nbsp; ");
	        $(topics).append("<div hidden=true class='topicbody' id='topicbody" + k + "'>" + tableHTML(body) + "</div>");
      }
      activate(0);
			})
		} else {
			$('#admin').empty();
			$('<iframe>')
				.addClass('fullSize')
				.attr('src', chartURL)
				.appendTo('#admin');		
		}
		return false;
	});

	//on pageload, show services
	loadServices();
	loadSidebar();

});

function formatUptime(time) {
	var m = time / 60000;

	if(m < 60) {
		return Math.round(m) + "m";
	} else if(m < 1440) {
		return Math.round(m/60) + "h";
	} else {
		return Math.round(m/1440) + "d";
	}
}

function sortAlpha(obj) {
	var list = [];
	$.each(obj, function(name, val){
		val.itemName = name;
		list.push(val)
	});
	list.sort(function(a, b) {
    return (a.itemName > b.itemName) ? 1 : -1;
	});
	return list;
}


function loadServices(){
	$.getJSON('/admin/snapshots', function(services){
		$('#admin').empty();
		var pubsub_max = maximum(values(services).map(function(service){ return pubsub_maximum(pubsub_counts(service)); }));
		sortAlpha(services).forEach(function(service){ createInstance(pubsub_max, service); });
	});
}

function loadSidebar(){  
	$.getJSON('/admin/sidebar-index', function(services){
		services.forEach(function(group){
			$('<h3>')
				.text(group[0])
				.appendTo('#sidebar');
			var ul = $('<ul>').appendTo('#sidebar');
			group[1].forEach(function(service){
				$('<li>')
					.html('<a href="'+service[1]+'">'+service[0]+'</a>')
					.appendTo(ul);
			});
		});
	})
}

function checkIfTable(data) {
	if (Object.keys(data).length <= 1) {
		return false;
	}
	var keys,
		isTable = true;
	$.each(data, function(name, val){
		if(typeof(val) != 'object') {
			return;
		}
		if(!keys) {
			keys = Object.keys(val)
		} else if(!arraysEqual(keys, Object.keys(val))) {
			isTable = false;
		}
	})
	return isTable;
}

// Return the maximum number in an array-like.
// [num] -> num
function maximum(ns){
    return ns.reduce(function(accum, count){ return Math.max(accum, count); }, 0);
}

// Return the values from an object.
// {k:v} -> [v]
function values(m){
    return Object.keys(m).map(function(k){ return m[k]; });
}

// Return a canvas element containing a log scale horizontal bar graph for each object in bars.
// [{color:str, value:num}] {width:num, height:num, maximum:num, bg_color:str} -> <canvas>
function horiz_log_bars(bars, conf){
    var jel = $('<canvas>').attr('width', conf.width).attr('height', conf.height);
    var ctx = jel[0].getContext('2d');
    var barh = conf.height / bars.length;
    ctx.save();
    if(conf.bg_color){
        ctx.fillStyle = conf.bg_color;
        ctx.fillRect(0, 0, conf.width, conf.height);
    }
    bars.forEach(function(bar, idx){
        ctx.fillStyle = bar.color;
        ctx.fillRect(0, barh * idx, Math.log(bar.value) / Math.log(conf.maximum) * conf.width, barh);
    });
    ctx.restore();
    return jel;
}

// Return a normalized pubsub summary for a service, despite optional keys in the key path. (JS doesn't have a "get-in")
// The key path is: service.last-snapshot[.pubsub-stats.counts{[.pub],[.sub]}]
// service json -> {'pub':{str:num}, 'sub':{str:num}}
function pubsub_counts(service){
    var pubsub_stats = service['last-snapshot']['pubsub-stats'] || {counts: {}};
    return {sub:pubsub_stats.counts.sub || {},
            pub:pubsub_stats.counts.pub || {}};
}

// Return the maximum pub or sub count in a pubsub_counts object.
// {'pub':{str:num}, 'sub':{str:num}} -> num
function pubsub_maximum(counts){
    return maximum(values(counts.sub).concat(values(counts.pub)));
}

// Make a vertically-narrow table describing quantities relative to conf.maximum
// {title:str, color:str, data:{str:num}} {width:num, height:num, maximum:num, bg_color:str} -> <table>
function quantity_table(series, conf){
    var fields = Object.keys(series.data);
    fields.sort(function(f1, f2){ return series.data[f2] - series.data[f1]; });
    return fields.reduce(function(table, field){
        table.append($('<tr>')
                     .append($('<td>').append(field))
                     .append($('<td>').addClass(series.cls).append(series.data[field]))
                     .append($('<td>').append(horiz_log_bars(
                         [{color:series.color, value:series.data[field]}], conf))));
        return table;
    }, $('<table>').addClass(series.cls).attr('cellspacing', 0).attr('cellpadding', 0));
}

// Summarize sub and pub data with two distribution tables.
// num {'pub':{str:num}, 'sub':{str:num}} -> <span>
function pubsub_summary_tables(max, counts){
    var barconf = {width:64, height:8, maximum:max, bg_color:'rgba(102,102,102,0.05)'};
    return [{title:'in', cls:'sub', color:'#faa', data:counts.sub},
            {title:'out', cls:'pub', color:'#666', data:counts.pub}].reduce(function(span, series){
                Object.keys(series.data).length && span.append(quantity_table(series, barconf));
                return span;
            }, $('<span>'));
}

function createInstance(pubsub_max, service) {
	var instance = $('<div>').addClass('instance'),
			infoLine = $('<div>').addClass('infoLine'),
			details = $('<div>').addClass('details');
	$('<h3>')
		.addClass('name')
		.text(service.itemName)
		.appendTo(infoLine);

        (function(){
            $('<span>')
                .addClass('pubsub')
                .append(pubsub_summary_tables(pubsub_max, pubsub_counts(service)))
                .appendTo(infoLine);
        })();

	$('<a>')
		.attr('href', '/admin/latest-snapshots?limit=10&service=' + service.itemName)
		.text('last 10')
		.appendTo(infoLine);

	$('<a>')
		.attr('href', '/admin/latest-snapshots?limit=1&service=' + service.itemName)
		.text('raw')
		.appendTo(infoLine);

	$('<div>')
		.addClass('uptime')
		.text("up " + formatUptime(service.info.uptime))
		.appendTo(infoLine);

	instance.append(infoLine);


	if(service['last-snapshot']) {
		sortAlpha(service['last-snapshot']).forEach(snapshotAttributes)
	}

	function snapshotAttributes(snap) {
		var name = snap.itemName;
		delete snap.itemName;
		$('<h3>')
			.text(name)
			.appendTo(details);

		if(checkIfTable(snap)) {
			var header = true;
			var table = $('<table>'),
					thead = '<thead><tr><th></th>',
					tbody = '<tbody>';
			$.each(snap, function(name, val){
				var tr = '<tr><td>' + name + '</td>';
				$.each(val, function(name, item){
					if(header) {
						thead += '<th>' + name + '</th>';
					}
					tr += '<td>' + JSON.stringify(item) + '</td>';
				});
				tr += '</tr>'
				tbody += tr;
				if(header) {
					thead += '</tr></thead>';
				}
				header = false;
			});
			tbody += '</tbody>';
			$(table)
				.html(thead + tbody)
				.appendTo(details);
		} else {
			var table = $('<table><thead><tr><th>key</th><th>val</th></tr></thead></table>'),
					tbody = $('<tbody>');
			sortAlpha(snap).forEach(function(val) {
				var name = val.itemName;
				delete val.itemName;
				$('<tr><td>'+name+'</td><td>'+JSON.stringify(val)+'</td></tr>')
					.appendTo(tbody);
			});
			$(table)
				.append(tbody)
				.appendTo(details);
		}

	}
	
	instance.append(details);


	$('#admin').append(instance);
}

function listHTML(tag, mapper, data) {
	var buffer = ["<" + tag + ">"];
	for(var i in data) buffer.push(mapper(data[i]));
	buffer.push["</" + tag + ">"]
	return buffer.join("");
}

function rowHTML(row) {
	return listHTML("tr", function(x){return "<td width='100'>" + x + "</td>";}, row);
}

function tableHTML(data) {
	return listHTML("table", rowHTML, data);
}

function activate(i) {
  $('.topicheader').css('font-weight', 'normal');
  $('#topicheader'+i).css('font-weight', 'bold');
  $('.topicbody').hide();
  $('#topicbody'+i).show();
}

function appendTimeseries(to, rows, w, h, mins) {
  var header=rows[0],
      rows=rows.slice(1),
      ns = rows[0].length - 1,
      p = 60,
      legp = 200, //(ns > 1 ? 100 : 0),
      maxx = rows[rows.length-1][0],
      x = d3.time.scale().domain([mins ? maxx - mins * 60 * 1000: rows[0][0], maxx]).range([0, w]),
      maxy = d3.max(rows, function(r) { return d3.max( r.slice(1)) ;}),
      y = d3.scale.linear().domain([0, 1.05*maxy]).range([h, 0]);

  var vis = d3.select(to)
      .data([rows])
    .append("svg")
      .attr("width", w + p * 2 + legp)
      .attr("height", h + p * 2)
    .append("g")
      .attr("transform", "translate(" + p + "," + p + ")");

  var xrules = vis.selectAll("g.xrule")
      .data(x.ticks(w / 80))
    .enter().append("g")
      .attr("class", "xrule");

  var yrules = vis.selectAll("g.yrule")
      .data(y.ticks(h/50))
    .enter().append("g")
      .attr("class", "yrule");

  xrules.append("line")
      .attr("x1", x)
      .attr("x2", x)
      .attr("y1", 0)
      .attr("y2", h - 1)
      .attr("stroke", "#eee")
      .attr("shape-rendering", "crispEdges");

  yrules.append("line")
      // .attr("class", function(d) { return d ? null : "axis"; })
      .attr("y1", y)
      .attr("y2", y)
      .attr("x1", 0)
      .attr("x2", w + 1)
      .attr("stroke", "#eee")
      .attr("shape-rendering", "crispEdges");

  xrules.append("text")
      .attr("x", x)
      .attr("y", h + 3)
      .attr("dy", ".71em")
      .attr("text-anchor", "middle")
      .text(x.tickFormat(10));

  yrules.append("text")
      .attr("y", y)
      .attr("x", -3)
      .attr("dy", ".35em")
      .attr("text-anchor", "end")
      .text(y.tickFormat(10));

  var colors = ["red", "black", "darkblue", "maroon", "aqua", "chartreuse", "coral", "darkgreen", "gray", "hotpink", "olive"]

  for(var i = 0; i < ns; i++) {
    vis.append("path")
        .attr("class", "line")
        .attr("fill", "none")
        .attr("stroke", colors[i])
        .attr("stroke-width", "1.5px")
        .attr("d", d3.svg.line()
        .x(function(d) { return x(d[0]); })
        .y(function(d) { return y(d[i+1]); }));
        
    vis.append("svg:rect")
        .attr("x", w + 5)
        .attr("y", 50 + 30 * i)
        .attr("stroke", colors[i])
        .attr("fill", colors[i])
        .attr("height", 2)
        .attr("width", 20);

    vis.append("svg:text")
        .attr("fill", colors[i])
        .attr("x", w + 30)
        .attr("y", 55 + 30 * i)
        .text(header[i+1]);
    
  }
  
  return vis;  
}


function arraysEqual(arr1, arr2) {
    if(arr1.length !== arr2.length)
        return false;
    for(var i = arr1.length; i--;) {
        if(arr1[i] !== arr2[i])
            return false;
    }

    return true;
}
