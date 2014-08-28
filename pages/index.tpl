<html>
<head>
<script src="bower_components/jquery/jquery.min.js"></script>
<script src="bower_components/jquery-json/dist/jquery.json.min.js"></script>
<script src="rdfquery/dist/js/jquery.rdfquery.core-1.0.js"></script>
<script src="rdfquery/jquery.rdf.turtle.js"></script>
</head>
<body>

#set( $datasets = $mgt.datasets($request) )

Dataset: <select name="dataset">
#foreach($ds in $datasets)
<option value="${ds}">${ds}</option>
#end
</select>
Graph: <input name="graph" value="http://test.drugis.org/person/Gert">
Time: <input name="time" value="2006-01-23T00:00:00">
<input type="button" onclick="go()" value="Go">

<pre id="test"></pre>

<script>
function go() {
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
</script>

</body>
</html>
