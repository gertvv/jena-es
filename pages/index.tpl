<html>
<head>
<script src="bower_components/jquery/jquery.min.js"></script>
<script src="bower_components/jquery-json/dist/jquery.json.min.js"></script>
<script src="rdfquery/dist/js/jquery.rdfquery.core-1.0.js"></script>
<script src="rdfquery/jquery.rdf.turtle.js"></script>

<style>
.form {
  border: 2px dashed grey;
  margin-top: 2em;
}

.historyEntry {
  background-color: #F2EB96;
}

.claims {
  background-color: #BFF296;
}

.retractions {
  background-color: #F296B5;
}
</style>

</head>
<body>

#set( $datasets = $mgt.datasets($request) )

Dataset: <select name="dataset">
#foreach($ds in $datasets)
<option value="${ds}">${ds}</option>
#end
</select>
Graph: <input name="graph" value="http://test.drugis.org/person/Gert">

<div class="form">
The graph at time = <input name="time" value="2006-01-23T00:00:00">
<input type="button" onclick="get()" value="Get">
<pre id="test" style="background-color: #F2EB96"></pre>
</div>

<div class="form">
Update the graph: <select name="writeMethod"><option value="POST">Add to the graph</option><option value="PUT">Overwrite the graph</option></select>
<div>
<textarea name="turtle" cols="80" rows="10">
@prefix foaf: &lt;http://xmlns.com/foaf/0.1/&gt;
@prefix person: &lt;http://test.drugis.org/person/&gt;

person:Gert foaf:homepage <http://orcid.org/0000-0001-9231-2770>;
</textarea>
</div>
<input type="button" onclick="update()" value="Update">
</div>

<div class="form">
The history of the graph 
<input type="button" onclick="history()" value="Get">
<div id="history"></div>
</div>

<div class="form">
SPARQL query at time = <input name="sparqlTime" value="2006-01-23T00:00:00">
<div>
<textarea name="sparql" cols="80" rows="10">
PREFIX foaf: &lt;http://xmlns.com/foaf/0.1/&gt;
PREFIX person: &lt;http://test.drugis.org/person/&gt;

SELECT ?mbox ?url
WHERE {
  GRAPH person:Gert {
    person:Gert foaf:homepage ?url;
    foaf:mbox ?mbox .
  }
}</textarea>
</div>
<input type="button" onclick="query()" value="Query">
<pre id="results" style="background-color: #F2EB96"></pre>
</div>

<script>
function query() {
  var dataset = $("select[name='dataset']").val();
  var time = $("input[name='sparqlTime']").val();
  var sparql = $("textarea[name='sparql']").val();
  console.log(sparql);
  $.ajax({
    url: dataset + "/query",
    data: { query: sparql, t: time },
    headers: { "Accept": "text/plain" },
    type: "GET",
    success: function(data, status) {
      $("#results").text(data);
    }
  });
}

function update() {
  var dataset = $("select[name='dataset']").val();
  var graph = $("input[name='graph']").val();
  var method = $("select[name='writeMethod']").val();
  var turtle = $("textarea[name='turtle']").val();
  console.log(dataset, graph, method, turtle);
  $.ajax({
    url: dataset + "/data?graph=" + graph,
    data: turtle,
    headers: { "Content-Type": "text/turtle" },
    type: method,
    success: function(data, status) {
      console.log("Whoopie");
    }
  });
}

function get() {
  var dataset = $("select[name='dataset']").val();
  var graph = $("input[name='graph']").val();
  var time = $("input[name='time']").val();
  console.log(dataset, graph, time);
  $.ajax({
    url: dataset + "/data",
    data: { graph: graph, t: time },
    headers: { "Accept": "application/rdf+xml" },
    type: "GET",
    success: function(data, status) {
      var x = $.rdf().load(data).databank;
      $("#test").text(x.dump({"format":"text/turtle", "indent": true}));
    }
  });
}

function fetchDelta(el, graph) {
  var dataset = $("select[name='dataset']").val();
  
  $.ajax({
    url: dataset + "/delta",
    data: { graph: graph },
    headers: { "Accept": "application/rdf+xml" },
    type: "GET",
    success: function(data, status) {
      var x = $.rdf().load(data).databank;
      $(el).text(x.dump({"format":"text/turtle", "indent": true}));
    }
  });
}

var id = 0;

function addHistory(item) {
  var claimsURI = item.claims ? item.claims.value._string : undefined;
  var retractionsURI = item.retractions ? item.retractions.value._string : undefined;

  var entry = document.createElement("div");
  entry.className = "historyEntry";
  entry.id = "history" + (++id);
  entry.innerHTML = item.delta.value.path + " by " + item.author.value.path + " at " + item.date.value;
  var fetch = document.createElement("input");
  fetch.type = "button";
  fetch.value = "Show";
  fetch.onclick = function() {
    if (claimsURI) {
      var el = document.createElement("pre");
      el.className = "claims";
      $(entry).append(el);
      fetchDelta(el, claimsURI);
    }
    if (retractionsURI) {
      var el = document.createElement("pre");
      el.className = "retractions";
      $(entry).append(el);
      fetchDelta(el, retractionsURI);
    }
  };
  $(entry).append(fetch);
  $("#history").append(entry);
}

function sortByDate(a, b) {
  var aDate = a.date;
  var bDate = b.date; 
  return ((aDate < bDate) ? -1 : ((aDate > bDate) ? 1 : 0));
}

function history() {
  var dataset = $("select[name='dataset']").val();
  var graph = $("input[name='graph']").val();
  console.log(dataset, graph);
  $("#history").empty();
  $.ajax({
    url: dataset + "/history",
    data: { graph: graph },
    headers: { "Accept": "application/rdf+xml" },
    type: "GET",
    success: function(data, status) {
      $.rdf().load(data)
        .prefix('dc', 'http://purl.org/dc/elements/1.1/')
        .prefix('stmt', 'http://test.drugis.org/ontology/statements#')
        .prefix('person', 'http://test.drugis.org/person/')
        .where('?delta dc:date ?date')
        .where('?delta dc:creator ?author')
        .optional('?delta stmt:claims ?claims')
        .optional('?delta stmt:retractions ?retractions')
        .matches.map(function(match) { return match.bindings; })
        .sort(sortByDate)
        .forEach(addHistory);
    }
  });
}
</script>

</body>
</html>
