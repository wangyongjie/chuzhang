var express = require('express');
var router = express.Router();
var async = require('async')
var fs = require('fs')
var URL = require('url');

var mysql = require('mysql');
var connection = mysql.createConnection({
  host : 'localhost',
  user : 'root',
  password : '123456',
  database : 'test'
});
connection.connect(); //建立连接


/* GET home page. */
router.get('/', function(req, res, next) {
  res.render('index', { title: 'Express' });
});

router.get('/gdf/one', function(req, res, next) {
  res.connection.setTimeout(0)

  var ftp = require('ftp')
  var params = URL.parse(req.url, true).query;  
  var fileName = params.param_one + '' + params.param_two
  var filePreName = getPreTime(params.param_one) + '' + params.param_two
  var log = []
  async.waterfall([
    function(cb){
      console.log(getTime() + '开始下载文件到本地')
      console.log(getTime() + '下载accitem.txt文件')
      var ftpClient = new ftp()
      ftpClient.on('ready', function() {
          ftpClient.get('./test/accitem.txt', function(err, stream) {
            if(err) {
              return cb(err)
            }

            stream.once('close', function() { 
              ftpClient.end(); 
              cb()
            });
            stream.pipe(fs.createWriteStream('./public/original/accitem.txt'));
          });
      });
      ftpClient.connect({
        host : '10.247.38.8',
        user : 'zj_zhdan',
        password : 'zj_zhdan'
      });
    },
    function(cb){
      console.log(getTime() + '下载ib_cbtm_fixfee.zj.' + fileName + '.psn文件')
      log.push(getTime() + '下载ib_cbtm_fixfee.zj.' + fileName + '.psn文件')
      var ftpClient = new ftp()
      ftpClient.on('ready', function() {
          ftpClient.get('./zjfile/gdf/ib_cbtm_fixfee.zj.' + fileName + '.psn', function(err, stream) {
            if(err) {
              return cb(err)
            }

            stream.once('close', function() { 
              ftpClient.end(); 
              cb()
            });
            stream.pipe(fs.createWriteStream('./public/original/ib_cbtm_fixfee.zj.' + fileName + '.psn'));
          });
      });
      ftpClient.connect({
        host : '10.247.38.8',
        user : 'zj_zhdan',
        password : 'zj_zhdan'
      });
    },
    function(cb){
      console.log(getTime() + '下载ib_cbtm_fixfee.zj.' + filePreName + '.psn文件')
      log.push(getTime() + '下载ib_cbtm_fixfee.zj.' + filePreName + '.psn文件')
      var ftpClient = new ftp()
      ftpClient.on('ready', function() {
          ftpClient.get('./zjfile/gdf/ib_cbtm_fixfee.zj.' + filePreName + '.psn', function(err, stream) {
            if(err) {
              return cb(err)
            }

            stream.once('close', function() { 
              console.log(getTime() + '结束下载文件到本地')
              log.push(getTime() + '结束下载文件到本地')
              ftpClient.end(); 
              cb()
            });
            stream.pipe(fs.createWriteStream('./public/original/ib_cbtm_fixfee.zj.' + filePreName + '.psn'));
          });
      });
      ftpClient.connect({
        host : '10.247.38.8',
        user : 'zj_zhdan',
        password : 'zj_zhdan'
      });
    },
  ], function (err, result) { 
    if(err) {
      console.log('【程序出错】', err)
      return res.send({ ok : 1, msg : 'fail', log : err })
    } 
    res.send({ ok : 0, msg : 'success', log : log })
  })
});

router.get('/gdf/two', function(req, res, next) {
  res.connection.setTimeout(0)

  var params = URL.parse(req.url, true).query;  
  var fileName = params.param_one + '' + params.param_two
  var filePreName = getPreTime(params.param_one) + '' + params.param_two
  var log = []

  async.waterfall([
    function(cb){
      console.log(getTime() + '开始导入费项和固定费文件')
      var sql = "DROP TABLE IF EXISTS `accitem`; " 

      connection.query(sql, function(err, rows, fields) {
        if(err) {
          return cb(err)
        }

        var sql = "create table accitem( " +
              "accitem varchar(30), " +
              "accitem_name varchar(30), " +
              "proid varchar(30), " +
              "proid_name varchar(90), " +
              "KEY `accitem` (`accitem`) USING BTREE " +
            " ) ENGINE=MyISAM DEFAULT CHARSET=gbk; " 

        connection.query(sql, function(err, rows, fields) {
          if(err) {
            return cb(err)
          }
          
            cb();
        });
      });
    },
    function(cb){
      console.log(getTime() + '导入费项文件accitem.txt')
      var sql = "LOAD DATA LOCAL INFILE './public/original/accitem.txt' " +
        "ignore INTO TABLE accitem character set gbk  " +
      "FIELDS TERMINATED BY '|'  " +
      // "OPTIONALLY ENCLOSED BY '\"'  " +
      "LINES TERMINATED BY '\r\n'  " 

      connection.query(sql, function(err, rows, fields) {
          if(err) {
          return cb(err)
        }

          cb();
      });
    },
    function(cb){
      var sql = "DROP TABLE IF EXISTS `tb_fixfee_hwyc`; " 

      connection.query(sql, function(err, rows, fields) {
        if(err) {
          return cb(err)
        }

        var sql = "create table tb_fixfee_hwyc( " +
               "SERVNUMBER        varchar(20) ," +
               "VALIDBILLCYC        float  ," +
               "SUBSID        float  ," +
               "ACCTITEM_ID        float  ," +
               "PAICLUP       decimal  ," +
               "ADJUST       float  ," +
               "RECEIVABLE        float  ," +
               "ISACCOUNT       float  ," +
               "PLANLIST        varchar(255) ," +
               "RULELIST       varchar(255) ," +
               "PAICLUPLIST       varchar(255) ," +
               "ISPAYED        float  ," +
               "DISPRODLIST       varchar(512) ," +
               "ACCTID       float  ," +
               "ISGROUP       float  ," +
                "KEY `SERVNUMBER` (`SERVNUMBER`) USING BTREE, " +
              "KEY `ACCTITEM_ID` (`ACCTITEM_ID`) USING BTREE, " +
                "KEY `PLANLIST` (`PLANLIST`) USING BTREE " +
            " ) ENGINE=MyISAM DEFAULT CHARSET=gbk; " 

        connection.query(sql, function(err, rows, fields) {
          if(err) {
            return cb(err)
          }
          
            cb();
        });
      });
    },
    function(cb){
      log.push(getTime() + '导入固定费文件ib_cbtm_fixfee.zj.' + fileName + '.psn文件')
      console.log(getTime() + '导入固定费文件ib_cbtm_fixfee.zj.' + fileName + '.psn文件')
      var sql = "LOAD DATA LOCAL INFILE './public/original/ib_cbtm_fixfee.zj." + fileName + ".psn' " +
        "ignore INTO TABLE tb_fixfee_hwyc character set gbk  " +
      "FIELDS TERMINATED BY '|'  " +
      // "OPTIONALLY ENCLOSED BY '\"'  " +
      "LINES TERMINATED BY '\r'  " 

      connection.query(sql, function(err, rows, fields) {
          if(err) {
          return cb(err)
        }

          cb();
      });
    },
    function(cb){
      log.push(getTime() + '导入固定费文件ib_cbtm_fixfee.zj.' + filePreName + '.psn文件')
      console.log(getTime() + '导入固定费文件ib_cbtm_fixfee.zj.' + filePreName + '.psn文件')
      var sql = "LOAD DATA LOCAL INFILE './public/original/ib_cbtm_fixfee.zj." + filePreName + ".psn' " +
          "ignore INTO TABLE tb_fixfee_hwyc character set gbk  " +
        "FIELDS TERMINATED BY '|'  " +
        // "OPTIONALLY ENCLOSED BY '\"'  " +
        "LINES TERMINATED BY '\r'  " 
      connection.query(sql, function(err, rows, fields) {
        if(err) {
          return cb(err)
        }

        log.push(getTime() + '结束导入费项和固定费文件')
        console.log(getTime() + '结束导入费项和固定费文件')
        cb();
      });
    },
  ], function (err, result) { 
    if(err) {
      console.log('【程序出错】', err)
      return res.send({ ok : 1, msg : 'fail', log : err })
    } 
    res.send({ ok : 0, msg : 'success', log : log })
  })
});

router.get('/gdf/three', function(req, res, next) {
  res.connection.setTimeout(0)

  var params = URL.parse(req.url, true).query;  
  var fileName = params.param_one + '' + params.param_two
  var filePreName = getPreTime(params.param_one) + '' + params.param_two
  var log = []
  
  async.waterfall([
    function(cb){
      console.log(getTime() + '开始生成固定费差异文件')
      var sql = "DROP TABLE IF EXISTS `tb_fixfee_hwyccy`; " 

      connection.query(sql, function(err, rows, fields) {
        if(err) {
          return cb(err)
        }
        
          cb();
      });
    },
    function(cb){
      var sql = "create table tb_fixfee_hwyccy  as " +
            "select a.ACCTITEM_ID, c.accitem_name, a.je as aje, a.cn as acn, b.je as bje, b.cn as bcn, (a.je-b.je)as jec, FORMAT((a.je-b.je)*100/b.je, 2)  as jeccy, " +
            "(a.cn-b.cn)as cnc, FORMAT((a.cn-b.cn)*100/b.cn, 2)  as cnccy " +
             "from " +
            "(select ACCTITEM_ID,count(*)cn,sum(PAICLUP)je from tb_fixfee_hwyc where validbillcyc = " + params.param_one + "00 group by ACCTITEM_ID)a LEFT JOIN " +
            "(select ACCTITEM_ID,count(*)cn,sum(PAICLUP)je from tb_fixfee_hwyc where validbillcyc = " + getPreTime(params.param_one) + "00 group by ACCTITEM_ID)b  " +
            "on a.ACCTITEM_ID = b.ACCTITEM_ID  " +
            "LEFT JOIN (select DISTINCT accitem as accitem_id, accitem_name from accitem) c " +
            "on a.ACCTITEM_ID = c.accitem_id ; " 
      console.log(sql)
      connection.query(sql, function(err, rows, fields) {
        if(err) {
          return cb(err)
        }

        var sql = "select * from tb_fixfee_hwyccy ORDER BY jec desc;"
        connection.query(sql, function(err, rows, fields) {
          if(err) {
            return cb(err)
          }
          
            cb(null, rows);
        });
      });
    },
    function(rows, cb){
      var xlsx = require('xlsx')
      var _headers = ['帐单科目标识', '帐单科目名称', '上月帐单金额', '上月记录数', '本月帐单金额','本月记录数'
              ,'差额（H-C）','差额百分比=k/C*100','记录数差额(I-D)','记录数差额百分比＝m/D*100','变化原因']

        var _data = []
      for(var i=0; i<rows.length; i++) {
        _data[i] = {}
        _data[i]['帐单科目标识'] = rows[i].ACCTITEM_ID || ''
        _data[i]['帐单科目名称'] = rows[i].accitem_name || ''
        _data[i]['上月帐单金额'] = rows[i].bje || ''
        _data[i]['上月记录数'] = rows[i].bcn || ''
        _data[i]['本月帐单金额'] = rows[i].aje || ''
        _data[i]['本月记录数'] = rows[i].acn || ''
        _data[i]['差额（H-C）'] = rows[i].jec || ''
        _data[i]['差额百分比=k/C*100'] = rows[i].jeccy || ''
        _data[i]['记录数差额(I-D)'] = rows[i].cnc || ''
        _data[i]['记录数差额百分比＝m/D*100'] = rows[i].cnccy || ''
        _data[i]['变化原因'] = ''
      }

          var headers = _headers
                  .map((v, i) => Object.assign({}, {v: v, position: String.fromCharCode(65+i) + 1 }))
                  .reduce((prev, next) => Object.assign({}, prev, {[next.position]: {v: next.v}}), {});
          var data = _data
                .map((v, i) => _headers.map((k, j) => Object.assign({}, { v: v[k], position: String.fromCharCode(65+j) + (i+2) })))
                .reduce((prev, next) => prev.concat(next))
                .reduce((prev, next) => Object.assign({}, prev, {[next.position]: {v: next.v}}), {});
          var output = Object.assign({}, headers, data);
          var outputPos = Object.keys(output);
          var ref = outputPos[0] + ':' + outputPos[outputPos.length - 1];

      var wb = {
          SheetNames: ['近往月固定费用比较(个人）'],
          Sheets: {
              '近往月固定费用比较(个人）': Object.assign({}, output, { '!ref': ref })
          }
      };

      var uploadDir = './public/result/';
      var file = params.param_one + '00固定费差异数据'
      var filePath = uploadDir + file + ".xlsx";
      xlsx.writeFile(wb, filePath);

      console.log(getTime() + '生成差异文件:' + filePath)
      console.log(getTime() + '结束生成固定费差异文件')
      log.push(getTime() + '生成差异文件:' + filePath)
      log.push(getTime() + '结束生成固定费差异文件')
      cb()
    },
  ], function (err, result) { 
    if(err) {
      console.log('【程序出错】', err)
      return res.send({ ok : 1, msg : 'fail', log : err })
    } 
    res.send({ ok : 0, msg : 'success', log : log })
  })
});

router.get('/gdf/four', function(req, res, next) {
  res.connection.setTimeout(0)

  var params = URL.parse(req.url, true).query;  
  var fileName = params.param_one + '' + params.param_two
  var filePreName = getPreTime(params.param_one) + '' + params.param_two
  var log = []

  async.waterfall([
    function(cb){
      console.log(getTime() + '开始检查固定费异常波动')
      var sql = "SELECT * from tb_fixfee_hwyccy where " +
                " (abs(jec) > 200000  and abs(jeccy) > 20) or (bje is null and aje > 50000)  ORDER BY jec DESC;"

      console.log(sql)
      connection.query(sql, function(err, rows, fields) {
          if(err) {
          return cb(err)
        }
        if(rows.length == 0) {
          console.log(getTime() + '无波动异常的费项')
          return cb(null, [])
        }

        console.log(getTime() + '存在波动异常的费项')
        // console.log('帐单科目标识','帐单科目名称','上月帐单金额','上月记录数','本月帐单金额','本月记录数','差额（H-C）','差额百分比=k/C*100','记录数差额(I-D','记录数差额百分比＝m/D*100')
        // for(var i=0; i<rows.length; i++) {
        //   console.log(rows[i].ACCTITEM_ID ,rows[i].accitem_name ,rows[i].aje ,rows[i].acn ,rows[i].bje ,rows[i].bcn ,rows[i].jec ,rows[i].jeccy ,rows[i].cnc ,rows[i].cnccy)
        // }

        cb(null, rows);
      });
    },
  ], function (err, result) { 
    if(err) {
      console.log('【程序出错】', err)
      return res.send({ ok : 1, msg : 'fail', log : err })
    } 
    console.log(getTime() + '结束检查固定费异常波动')
    res.send({ ok : 0, msg : 'success', log : log, rows : result })
  })
});

router.get('/gdf/five', function(req, res, next) {
  res.connection.setTimeout(0)

  var params = URL.parse(req.url, true).query;  
  var fileName = params.param_one + '' + params.param_two
  var filePreName = getPreTime(params.param_one) + '' + params.param_two
  var log = []

  async.waterfall([
    function(cb){
      var sql = "SELECT * from tb_fixfee_hwyccy where " +
                " (abs(jec) > 200000  and abs(jeccy) > 20) or (bje is null and aje > 50000)  ORDER BY jec DESC;"

      connection.query(sql, function(err, rows, fields) {
          if(err) {
          return cb(err)
        }
        if(rows.length == 0) {
          console.log('无波动异常的费项')
          return cb(null, [])
        }

        cb(null, rows);
      });
    },
    function(rows, cb) {
      if(rows.length == 0) {
        return cb()
      }
      var acct = "("
      for(var i=0; i<rows.length; i++) {
        if(i == rows.length -1) {
          acct += rows[i].ACCTITEM_ID + ")"
        }else {
          acct += rows[i].ACCTITEM_ID + ","
        }
      }
      console.log(getTime(), '开始生成异常波动分析')
      var sql = "SELECT a.ACCTITEM_ID, a.PLANLIST, b.proid_name, a.VALIDBILLCYC, a.cn , a.je FROM " +
          " (select ACCTITEM_ID, PLANLIST, VALIDBILLCYC, count(*) as cn, SUM(PAICLUP) as je from tb_fixfee_hwyc where ACCTITEM_ID in " +
          acct + " GROUP BY ACCTITEM_ID, PLANLIST, VALIDBILLCYC having SUM(PAICLUP) > 20000 ) a " +
          " LEFT JOIN " +
          " (SELECT DISTINCT proid, proid_name FROM accitem) b " +
          " ON a.PLANLIST = b.proid; " 

      console.log(sql)
      console.log('费项', acct, '的波动分析如下：')
      console.log('费项id', '产品id', '产品名称', '账期', '记录数', '金额')
      connection.query(sql, function(err, rows, fields) {
          if(err) {
          return cb(err)
        }
        if(rows.length == 0) {
          console.log('无波动分析')
          return cb()
        }

        for(var i=0; i<rows.length; i++) {
          console.log(rows[i].ACCTITEM_ID, rows[i].PLANLIST, rows[i].proid_name, rows[i].VALIDBILLCYC, rows[i].cn, rows[i].je)
        }
        console.log(getTime() + '结束生成异常波动分析')

        cb(null, rows);
      });
    }
  ], function (err, result) { 
    if(err) {
      console.log('【程序出错】', err)
      return res.send({ ok : 1, msg : 'fail', log : err })
    } 
    res.send({ ok : 0, msg : 'success', log : log, rows : result })
  })
});

function getTime() {
    var d = new Date()
    return '【' + d.getFullYear() + '-' + (d.getMonth() + 1) + '-' + d.getDate() + ' '
             + d.getHours() + ':' + d.getMinutes() + ':' + d.getSeconds() + ':' + ('0000'+d.getMilliseconds()).slice(-3) + '】'
}
function getPreTime(temp) {
  var year = parseInt(temp.substring(0,4))
  var day  = parseInt(temp.substring(4))
  if(day == 1){
    day  = 12
    year = year - 1
  }else if(day < 11){
    day = day - 1
    day = '0' + day
  }else{
    day = day -1
  }

  return year + '' + day
}

module.exports = router;

