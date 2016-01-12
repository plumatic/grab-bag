document.sortable_table = (function(){
    
    function byid(s){
        // look up an element by id
        return document.getElementById(s);
    }

    function kill(s){
        // remove an element from the dom by id
        var e = byid(s);
        e && e.parentNode.removeChild(e);
    }

    function renderTag(v){
        // render an html tag according to the clojure format [tag & [{attr-name attr-value}] more]
        var attr_flag = v[1] && !v[1].map && typeof v[1] == 'object';
        var attrm = attr_flag ? v[1] : {};
        var more = v.slice(attr_flag ? 2 : 1);
        var e = document.createElement(v[0]);
        Object.keys(attrm).forEach(function(k){ e.setAttribute(k, attrm[k]); });
        more.forEach(function(w){
            e.appendChild(w.map ? renderTag(w) : document.createTextNode(w));
        });
        return e;
    }

    function renderRow(m, ks){
        // render a table row ordered by keys, ks, from the given clojure map, m
        return renderTag(['tr'].concat(ks.map(function(k){ return ['td', m[k]]; })));
    }

    function cmp_val(v, td){
        // generate a comparison value from v or td (td is the rendered version of v)
        switch(typeof v){
        case 'number': return v;
        case 'string': return v.toLocaleLowerCase();
        case 'object': return td.innerText.toLocaleLowerCase();
        default: throw 'cmp_val: do not recognize ' + typeof v;
        };
    }

    function simple_index_cmp(index, lookup){
        // make a simple cmp function according to the type of index
        var cmp = Intl.Collator('en', {sensitivity: 'base'}).compare;
        switch(typeof index[0]){
        case 'number': return function(a, b){ return lookup(index, a) - lookup(index, b); };
        case 'string': return function(a, b){ return cmp(lookup(index, a), lookup(index, b)); };
        default: throw 'simple_index_cmp: do not recognize ' + typeof v;
        };
    }

    function index_cmp(indexm, keys, k0, k1, asc, lookup){
        // return a function to
        //  - compare according to precomputed indexed comparison values for k0
        //  - use a fallback index k1 to break ties
        //  - sort in asc or desc order
        var cmp0 = simple_index_cmp(indexm[k0], lookup);
        var cmp1 = simple_index_cmp(indexm[k1], lookup);
        var factor = asc ? 1 : -1;
        return function(a, b){ return factor * (cmp0(a, b) || cmp1(a, b)); };
    }

    function snarf_data(id){
        // ! parse a json element in the dom and then delete it
        var data = JSON.parse(byid(id).textContent);
        kill(id);
        return data;
    }

    function init_data(keys, data_id, first_sort_key, first_sort_asc){
        // ! parse, offscreen-render, and index the sortable table data
        var ascm = {};
        var indexm = {};
        keys.forEach(function(k, i){
            ascm[k] = true;
            indexm[k] = [];
        });
        ascm[first_sort_key] = first_sort_asc;
        var trs = snarf_data(data_id).map(function(x, ix){
            var tr = renderRow(x, keys);
            tr.data_index = ix;
            keys.forEach(function(k, ik){
                indexm[k].push(cmp_val(x[k], tr.childNodes[ik]));
            });
            return tr;
        });
        return {ascm:ascm, indexm:indexm, trs:trs};
    }

    return function(keys, data_id, draw_ct, sort_tiebreaker_key, first_sort_key, first_sort_asc, debug){
        // ! main function for a running sortable table
        var parent = byid(data_id).parentElement;
        var data = init_data(keys, data_id, first_sort_key, first_sort_asc);
        var id = { container:'container_' + data_id
                   , status:'status_' + data_id
                   , table:'table_' + data_id
                 };
        var call = { draw: function(ct){ return 'document["' + data_id + '"].draw(' + ct + ');'; }
                     , resort: function(k){ return 'document["' + data_id + '"].resort("' + k + '");'; }
                   };
        var draw_list = [];
        function reinsert(ih, asc){
            // ! delete and recreate the table with one header row
            kill(id.container);
            parent.appendChild(renderTag(
                ['div', {id:id.container,style:'padding:16px 0px;'},
                 ['a', {href:'#' + id.status}, 'jump to bottom'],
                 ['table', {border:1,cellspacing:0,cellpadding:3,width:'100%',id:id.table},
                  ['tr'].concat(keys.map(function(k, ik){
                      return ['td', {style:'white-space:nowrap;'},
                              ['button', {onclick:call.resort(k)}, k,
                               ih == ik ? (asc ? ' \u25bc' : ' \u25b2') : '',
                               k == sort_tiebreaker_key ? ' \u25cf' : '']];
                  }))],
                 ['span', {id:id.status}, '-'],
                 ' ',
                 ['button', {onclick:call.draw(draw_ct) + 'this.blur();'}, 'draw ' + draw_ct + ' more'],
                 ' ',
                 ['a', {href:'#' + id.container}, 'back to top'],
                 ' . . . ',
                 ['button', {onclick:call.draw(data.trs.length) + 'this.blur();'}, 'draw all (slow)']]));
        }
        function draw(ct){
            // ! draw ct items from draw_list onto table by id
            if(draw_list.length){
                var t = byid(id.table);
                draw_list.splice(0, ct).forEach(function(tr){ t.appendChild(tr); });
                byid(id.status).innerText = draw_list.length + ' to draw';
            }
        }
        function resort(k){
            // ! sort the table using indexes, using asc/desc state, and redraw from offscreen html
            reinsert(keys.indexOf(k), data.ascm[k]);
            draw_list = data.trs.slice().sort(index_cmp(data.indexm, keys, k, sort_tiebreaker_key, data.ascm[k],
                                                        function(index, tr){ return index[tr.data_index]; }));
            data.ascm[k] = !data.ascm[k];
            draw(draw_ct);
        }
        resort(first_sort_key);
        debug && console.log(data_id, data);
        return {draw:draw, resort:resort};
    }
    
})();
