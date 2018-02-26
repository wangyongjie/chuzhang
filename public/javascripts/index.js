$(function() {
	$('.ui.dropdown')
	  .dropdown()
	;

	$('.gdf_submit').click(function() {
		$('.gdf_submit').attr("disabled","disabled");
		$(".result").empty()
		download(function() {
			loaddata(function() {
				stepThree(function() {
					stepFour(function() {
						stepFive(function() {
							$('.gdf_submit').removeAttr("disabled");
						})
					})
				})
			})
		})
	})

	function download(cb) {
		$(".result").append("<p>" + getTime() + "开始下载文件到本地</p>");  
  	$(".result").append("<p>" + getTime() + "下载accitem.txt文件</p>"); 

    $.ajax({
      type: 'GET',
      url: '/gdf/one',
      timeout : 0, 
      data: getPar(),
      dataType: 'json',
      success: function(result) {
        if(result.ok == '0') {
        	result.log.forEach(function (value) {
			      $(".result").append("<p>" + value + "</p>"); 
					})
					cb()
        } else {   
      		$(".result").append("<p>程序出错</p>"); 
		      $(".result").append("<p>" + result.log + "</p>"); 
        }
      }
    })
	}

	function loaddata(cb) {
		$(".result").append("<p>" + getTime() + "开始导入费项和固定费文件</p>");  
  	$(".result").append("<p>" + getTime() + "导入费项文件accitem.txt</p>"); 
  	$.ajax({
      type: 'GET',
      url: '/gdf/two',
      timeout : 0, 
      data: getPar(),
      dataType: 'json',
      success: function(result) {
        if(result.ok == '0') {
        	result.log.forEach(function (value) {
			      $(".result").append("<p>" + value + "</p>"); 
					})
					cb()
        } else {   
      		$(".result").append("<p>程序出错</p>"); 
		      $(".result").append("<p>" + result.log + "</p>"); 
        }
      }
    })
	}

	function stepThree(cb) {
		$(".result").append("<p>" + getTime() + "开始生成固定费差异文件</p>");  
  	$.ajax({
      type: 'GET',
      url: '/gdf/three',
      timeout : 0, 
      data: getPar(),
      dataType: 'json',
      success: function(result) {
        if(result.ok == '0') {
        	result.log.forEach(function (value) {
			      $(".result").append("<p>" + value + "</p>"); 
					})
					cb()
        } else {   
      		$(".result").append("<p>程序出错</p>"); 
		      $(".result").append("<p>" + result.log + "</p>"); 
        }
      }
    })
	}

	function stepFour(cb) {
		$(".result").append("<p>" + getTime() + "开始检查固定费异常波动</p>");  
  	$.ajax({
      type: 'GET',
      url: '/gdf/four',
      timeout : 0, 
      data: getPar(),
      dataType: 'json',
      success: function(result) {
        if(result.ok == '0') {
        	if(result.rows.length == 0) {
        		$(".result").append("<p>" + getTime() + "无波动异常的费项</p>")
        		$(".result").append("<p>" + getTime() + "结束检查固定费异常波动</p>")
        	}else{
        		showTable("1", result.rows)
	        	$(".result").append("<p>" + getTime() + "结束检查固定费异常波动</p>")
        		cb()
        	}
        } else {   
      		$(".result").append("<p>程序出错</p>"); 
		      $(".result").append("<p>" + result.log + "</p>"); 
        }
      }
    })
	}

	function stepFive(cb) {
		$(".result").append("<p>" + getTime() + "开始生成异常波动分析</p>");  
  	$.ajax({
      type: 'GET',
      url: '/gdf/five',
      timeout : 0, 
      data: getPar(),
      dataType: 'json',
      success: function(result) {
        if(result.ok == '0') {
        	if(result.rows.length == 0) {
        		$(".result").append("<p>" + getTime() + "无波动分析</p>")
        	}else{
        		showTable("2", result.rows)
        		cb()
        	}
        	$(".result").append("<p>" + getTime() + "结束生成异常波动分析</p>")
        } else {   
      		$(".result").append("<p>程序出错</p>"); 
		      $(".result").append("<p>" + result.log + "</p>"); 
        }
      }
    })
	}

	function showTable(type, rows) {
		var temp = "<table class='ui celled table'>"
		if(type == "1") {
			temp = temp + "<thead><tr>" +
						"<th>帐单科目标识</th>" +
						"<th>帐单科目名称</th>" +
						"<th>上月帐单金额</th>" +
						"<th>上月记录数</th>" +
						"<th>本月帐单金额</th>" +
						"<th>本月记录数</th>" +
						"<th>差额(元）</th>" +
						"<th>差额百分比(%)</th>" +
						"<th>记录数差额</th>" +
						"<th>记录数差额百分比(%)</th>" +
						"</tr></thead>"
			temp = temp + "<tbody>"
			for(var i=0; i<rows.length; i++) {
				temp = temp + "<tr>" + 
	      			"<td>" + rows[i].ACCTITEM_ID + "</td>" + 
	      			"<td>" + rows[i].accitem_name + "</td>" + 
	      			"<td>" + rows[i].bje + "</td>" + 
	      			"<td>" + rows[i].bcn + "</td>" + 
	      			"<td>" + rows[i].aje + "</td>" + 
	      			"<td>" + rows[i].acn + "</td>" + 
	      			"<td>" + rows[i].jec + "</td>" + 
	      			"<td>" + rows[i].jeccy + "</td>" + 
	      			"<td>" + rows[i].cnc + "</td>" +
	      			"<td>" + rows[i].cnccy + "</td>" +
	      			"</tr>"
	    }
	    temp = temp + "</tbody></table>"
	    $(".result").append("<p>" + getTime() + "存在波动异常的费项，如下：</p>")
		}else if (type == '2') {
			temp = temp + "<thead><tr>" +
						"<th>费项id</th>" +
						"<th>产品id</th>" +
						"<th>产品名称</th>" +
						"<th>账期</th>" +
						"<th>记录数</th>" +
						"<th>金额(元)</th>" +
						"</tr></thead>"
			temp = temp + "<tbody>"
			for(var i=0; i<rows.length; i++) {
				temp = temp + "<tr>" + 
							"<td>" + rows[i].ACCTITEM_ID + "</td>" +
							"<td>" + rows[i].PLANLIST + "</td>" +
							"<td>" + rows[i].proid_name + "</td>" +
							"<td>" + rows[i].VALIDBILLCYC + "</td>" +
							"<td>" + rows[i].cn + "</td>" +
							"<td>" + rows[i].je + "</td>" +
	      			"</tr>"
	    }
	    temp = temp + "</tbody></table>"
	    $(".result").append("<p>" + getTime() + "异常费项的分析如下：</p>")
		}
		
		$(".result").append(temp)
	}

	function getTime() {
    var d = new Date()
    return '【' + d.getFullYear() + '-' + (d.getMonth() + 1) + '-' + d.getDate() + ' '
             + d.getHours() + ':' + d.getMinutes() + ':' + d.getSeconds() + ':' + ('0000'+d.getMilliseconds()).slice(-3) + '】'
	}
	function getPar() {
		var temp = {}
		temp.param_one = $('.param_one').dropdown('get value')
		temp.param_two = $('.param_two').dropdown('get value')
		return temp
	}
})