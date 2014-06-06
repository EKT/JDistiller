function bytesToSize(bytes, precision)
{  
    var kilobyte = 1024;
    var megabyte = kilobyte * 1024;
    var gigabyte = megabyte * 1024;
    var terabyte = gigabyte * 1024;
   
    if ((bytes >= 0) && (bytes < kilobyte)) {
        return bytes + ' B';
 
    } else if ((bytes >= kilobyte) && (bytes < megabyte)) {
        return (bytes / kilobyte).toFixed(precision) + ' KB';
 
    } else if ((bytes >= megabyte) && (bytes < gigabyte)) {
        return (bytes / megabyte).toFixed(precision) + ' MB';
 
    } else if ((bytes >= gigabyte) && (bytes < terabyte)) {
        return (bytes / gigabyte).toFixed(precision) + ' GB';
 
    } else if (bytes >= terabyte) {
        return (bytes / terabyte).toFixed(precision) + ' TB';
 
    } else {
        return bytes + ' B';
    }
}

function create_nodes(nodes) {
               var html_code = '<div id="accordion">';
               for(var i=0;i < nodes.length; i++)
               {
                   html_code += '<h3><a href="#">'+nodes[i]+'</a></h3>'+'<div id="'+nodes[i]+'"></div>';
               }
               html_code +='</div>';
               return html_code;
}

function create_template(div_id) {
              var div_code = '<div id="'+div_id+'.phymem" style="width: 150px; height:150px;display:inline-block;"></div>';
              div_code += '<div id="'+div_id+'.load" style="width: 150px; height:150px;display:inline-block;"></div>';
              div_code += '<div id="'+div_id+'.virmem" style="width: 150px; height:150px;display:inline-block;"></div>';
              div_code += '<div id="'+div_id+'.disk_usage" style="width: 150px; height:150px;display:inline-block;"></div>';
              div_code += '<div id="'+div_id+'.disk_net_io" style="width: 150px; height:150px;display:inline-block;vertical-align:text-bottom;"></div>';
              div_code += '<div id="'+div_id+'.status" style="width: 100px; height:150px;display:inline-block;vertical-align:text-bottom;"></div>';
              document.getElementById(div_id).innerHTML = div_code;
} 

function create_pie(div_id,title) {
    var chart;
    chart = new Highcharts.Chart({
        chart: {
            renderTo: div_id,
            plotBackgroundColor: null,
            plotBorderWidth: null,
            plotShadow: false,
            width: 150,
            height: 150
        },
        title: { text: title },
        credits: { enabled: false },
        tooltip: {
            formatter: function() {
                return '<b>'+ this.point.name +'</b>: '+ Math.round(this.percentage) +' %'+'<br>'+bytesToSize(this.y,2);
                
            }
        },
        plotOptions: {
            pie: {
                allowPointSelect: true,
                cursor: 'pointer',
                dataLabels: {
                    enabled: false,
                    color: '#000000',
                    connectorColor: '#000000',
                    formatter: function() {
                        return '<b>'+ this.point.name +'</b>: '+ this.percentage +' %';
                    }
                }
            }
        },
        series: [{
            type: 'pie',
            name: 'Disk Usage',
            data: [
                {name: 'Free',y: 50.0,sliced: false,selected: false},
                {name: 'Used',y: 50.0,sliced: false,selected: false}
            ]
        }]
      });
      return chart;
}

function create_bar(div_id,title,cores,cpu_data) {
    var chart;
    chart = new Highcharts.Chart({
            chart: {
                renderTo: div_id,
                type: 'column',
                width: 150,
                height: 150
            },
            title: { text: title },
            credits: {enabled: false },
            subtitle: { text: '' },
            xAxis: { categories:  cores},
            yAxis: { min: 0, title: {text: ''} },
            legend: { enabled: false,layout: 'vertical', backgroundColor: '#FFFFFF', align: 'left', verticalAlign: 'top', x: 100, y: 70, floating: true, shadow: true },
            tooltip: {
                formatter: function() {
                    return ''+
                        this.series.name +': '+ this.y +' %';
                }
            },
            plotOptions: {
                column: {
                    pointPadding: 0.2,
                    borderWidth: 0
                }
            },
            series: cpu_data
        });
      return chart;
}
    
function get_attr(div_id,element)
{
      return div_id.replace(/[.]/gi,'_')+'_'+element;
}
