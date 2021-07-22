/* = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =\
||   This script for customer center (Time)                     ||
||                                                              ||
||  File Name: LMRY_CO_InvBalance_SCHDL_v2.0.js                ||
||                                                              ||
||  Version Date         Author        Remarks                  ||
||  2.0    NOVIEMBRE 09 2018  LatamReady    Use Script 2.0       ||
 \= = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = */
/**
 * @NApiVersion 2.x
 * @NScriptType ScheduledScript
 * @NModuleScope Public
 */
define(["N/record", "N/runtime", "N/file", "N/encode", "N/search",
  "N/format", "N/log", "N/config", "./CO_Library_Mensual/LMRY_CO_Reportes_LBRY_V2.0.js", "N/task"
],

  function (recordModulo, runtime, fileModulo, encode, search, format, log,
    config, libreria, task) {

    var objContext = runtime.getCurrentScript();
    //var namereport = "Reportes Libro de Inventario y Balance";
    var LMRY_script = 'LMRY CO Reportes Libro de Inventario y Balance SCHDL 2.0';
    //Parametros
    var paramSubsidi = '';
    var paramPeriodo = '';
    var paramPeriodsRestantes = '';
    var paramLogId = '';
    var paramMulti = '';
    var paramPUC = '';
    var paramFile = '';
    var paramAdjustment = '';

    //Control de Reporte
    var periodstartdate = '';
    var periodenddate = '';
    var companyruc = '';
    var companyname = '';

    var xlsString = '';

    var ArrMovimientos = new Array();
    var SaldoAnteriorPUC = new Array();
    var arrAccountingContext = new Array();
    var ArrAccounts = new Array();
    var SaldoAnterior = new Array();

    var periodname = '';
    var Final_string;
    var multibookName = '';
    var language;
    var Fecha_Corte_al;
    var PeriodosRestantes = new Array();
    var Pucs = new Array();
    //Features
    var featSubsi = null;
    var featMulti = null;

    function execute(context) {
      try {
        ObtenerParametrosYFeatures();
        PeriodosRestantes = paramPeriodsRestantes.split(',');
        PeriodosRestantes = PeriodosRestantes.map(function e(p) {
          return Number(p)
        });
        //obtener saldo anterior
        obtenerDataAnterior(); //obtiene data de archivo temporal, seteando en 2 variables SaldoAnteriorPUC, SaldoAnterior
        //obtener Movimientos
        if (PeriodosRestantes.length != 0) {

          ArrMovimientos = ObtieneTransacciones();
          log.debug('ArrMovimientos', ArrMovimientos);
          var dataEndJournal = ObtienePeriodEndJournal();
          log.debug('dataEndJournal', dataEndJournal);
          ArrMovimientos = ArrMovimientos.concat(dataEndJournal);

          if (featMulti) {
            ArrAccounts = ObtenerCuentas();
            ArrMovimientos = SetPUCMultibook(ArrMovimientos); //usa ArrAccounts
            log.debug('ArrMovimientos SetPUCMultibook', ArrMovimientos);
          }

          if (featMulti) {
            var ArrMovimientosSpecific = ObtieneSpecificTransaction();
            log.debug('ArrMovimientos specific', ArrMovimientos);
            ArrMovimientos = ArrMovimientos.concat(ArrMovimientosSpecific);

            if (!ValidatePrimaryBook() || ValidatePrimaryBook() != 'T') {
              var array_context = ObtieneAccountingContext();
              CambioDeCuentas(ArrMovimientos); //usa arrAccountingContext
            }
          }

          if (ArrMovimientos.length > 1) {
            ArrMovimientos = OrdenarCuentas(ArrMovimientos);
            log.debug('ArrMovimientos ordenadas', ArrMovimientos);
            ArrMovimientos = AgruparCuentas(ArrMovimientos);
            log.debug('Data Movimientos en Año de generación agrupada 4D', ArrMovimientos); //hasta el periodo de generación incluido
          }
        }
        //juntar arreglos de movimiento y saldo anterior
        Pucs = obtenerDescripPUC();
        var arrTotal = juntarSaldoYMovimiento(SaldoAnteriorPUC, ArrMovimientos);
        var dataTotal = SaldoAnterior.concat(arrTotal);
        log.debug('dataTotal', dataTotal);
        if (paramPUC == '9') {
          ObtenerDatosSubsidiaria();
          var arr2digits = agruparNivel2(dataTotal);
          log.debug('arr2digits', arr2digits);
          var arr1digits = agruparNivel1(arr2digits);
          log.debug('arr1digits', arr1digits);
          //Generar excel final del reporte
          if (arr1digits.length != 0) {
            GenerarExcel(dataTotal, arr2digits, arr1digits);
          } else {
            RecordNoData();
          }
        } else {
          //actualizar archivo temporal
          if (dataTotal.length != 0) {
            var nameFile = 'INVENTARIO_BALANCE_TEMPORAL';
            saveFile(formatear(dataTotal), nameFile, 'txt');
          } else {
            log.debug('No hay data ni de saldos ni de movimientos.', 'No se actualiza archivo.')
          }
          //llamar de nuevo a map reduce con el siguiente numero PUC
          paramPUC++;
          llamarMapReduce();
        }

      } catch (err) {
        libreria.sendMail(LMRY_script, ' [ execute ] ' + err);
        //var varMsgError = 'No se pudo procesar el Schedule.';
      }

    }

    function ObtienePeriodEndJournal() {
      // Control de Memoria
      var intDMaxReg = 1000;
      var intDMinReg = 0;
      var arrQuiebre = new Array();
      // Exedio las unidades
      var DbolStop = false;
      var arrCuatroDigitos = new Array();

      var savedsearch = search.load({
        id: 'customsearch_lmry_co_invent_balanc_PEJ'
      });

      var pucFilter = search.createFilter({
        name: 'formulatext',
        formula: '{account.custrecord_lmry_co_puc_d4_id}',
        operator: search.Operator.STARTSWITH,
        values: [paramPUC]
      });
      savedsearch.filters.push(pucFilter);

      var periodosSTR = PeriodosRestantes.toString();
      var periodFilterFROM = search.createFilter({
        name: 'formulanumeric',
        formula: 'CASE WHEN {postingperiod.id} IN (' + periodosSTR + ') THEN 1 ELSE 0 END',
        operator: search.Operator.EQUALTO,
        values: [1]
      });
      savedsearch.filters.push(periodFilterFROM);

      if (featSubsi) {
        var subsidiaryFilter = search.createFilter({
          name: 'subsidiary',
          operator: search.Operator.IS,
          values: [paramSubsidi]
        });
        savedsearch.filters.push(subsidiaryFilter);
      }

      if (featMulti) {
        var multibookFilter = search.createFilter({
          name: 'accountingbook',
          join: 'accountingtransaction',
          operator: search.Operator.IS,
          values: [paramMulti]
        });
        savedsearch.filters.push(multibookFilter);
        //11.
        var balanceColumn = search.createColumn({
          name: 'formulacurrency',
          summary: "SUM",
          formula: "NVL({accountingtransaction.debitamount},0) - NVL({accountingtransaction.creditamount},0)"
        });
        savedsearch.columns.push(balanceColumn);
      }

      var searchresult = savedsearch.run();

      while (!DbolStop) {
        var objResult = searchresult.getRange(intDMinReg, intDMaxReg);

        if (objResult != null) {
          var intLength = objResult.length;

          if (intLength != 1000) {
            DbolStop = true;
          }

          for (var i = 0; i < intLength; i++) {
            var columns = objResult[i].columns;
            arrQuiebre = new Array();

            for (var col = 0; col < columns.length; col++) {
              if (col == 7) {
                if (featMulti) {
                  arrQuiebre[col] = objResult[i].getValue(columns[11]);
                } else {
                  arrQuiebre[col] = objResult[i].getValue(columns[7]);
                }
              } else {
                arrQuiebre[col] = objResult[i].getValue(columns[col]);
              }
            }

            arrCuatroDigitos.push(arrQuiebre);
          }

          intDMinReg = intDMaxReg;
          intDMaxReg += 1000;

          if (intLength < 1000) {
            DbolStop = true;
          }
        } else {
          DbolStop = true;
        }
      }

      return arrCuatroDigitos;
    }

    function agruparNivel2(arrData) {
      var resultTot = new Array();
      var importe = 0;
      for (var i = 0; i < arrData.length; i++) {
        if (i == 0) {
          importe = Number(arrData[i][3]);
        } else if (i != arrData.length - 1) {
          if (arrData[i][0].substring(0, 2) == arrData[i - 1][0].substring(0, 2)) {
            importe += Number(arrData[i][3]);
            importe = redondear(importe);
          } else {
            var result = new Array();
            result.push(arrData[i - 1][0].substring(0, 2)); //puc
            result.push(arrData[i - 1][5]); //DESCRIPCION
            result.push(importe); //importe
            result.push(arrData[i - 1][6]); //decripcion puc 1d
            resultTot.push(result);
            importe = Number(arrData[i][3]);
          }
        } else {
          if (arrData[i][0].substring(0, 2) == arrData[i - 1][0].substring(0, 2)) {
            importe += Number(arrData[i][3]);
            importe = redondear(importe);
          } else {
            var result = new Array();
            result.push(arrData[i - 1][0].substring(0, 2)); //puc
            result.push(arrData[i - 1][5]); //DESCRIPCION
            result.push(importe); //importe
            result.push(arrData[i - 1][6]); //decripcion puc 1d
            resultTot.push(result);
            importe = Number(arrData[i][3]);
          }
          var result = new Array();
          result.push(arrData[i][0].substring(0, 2)); //puc
          result.push(arrData[i][5]); //DESCRIPCION
          result.push(importe); //importe
          result.push(arrData[i][6]); //decripcion puc 1d
          resultTot.push(result);
        }
      }
      return resultTot;
    }

    function agruparNivel1(arrData) {
      var resultTot = new Array();
      var importe = 0;
      for (var i = 0; i < arrData.length; i++) {
        if (i == 0) {
          importe = Number(arrData[i][2]);
        } else if (i != arrData.length - 1) {
          if (arrData[i][0].substring(0, 1) == arrData[i - 1][0].substring(0, 1)) {
            importe += Number(arrData[i][2]);
            importe = redondear(importe);
          } else {
            var result = new Array();
            result.push(arrData[i - 1][0].substring(0, 1)); //puc
            result.push(arrData[i - 1][3]); //DESCRIPCION
            result.push(importe); //importe
            resultTot.push(result);
            importe = Number(arrData[i][2]);
          }
        } else {
          if (arrData[i][0].substring(0, 1) == arrData[i - 1][0].substring(0, 1)) {
            importe += Number(arrData[i][2]);
            importe = redondear(importe);
          } else {
            var result = new Array();
            result.push(arrData[i - 1][0].substring(0, 1)); //puc
            result.push(arrData[i - 1][3]); //DESCRIPCION
            result.push(importe); //importe
            resultTot.push(result);
            importe = Number(arrData[i][2]);
          }
          var result = new Array();
          result.push(arrData[i][0].substring(0, 1)); //puc
          result.push(arrData[i][3]); //DESCRIPCION
          result.push(importe); //importe
          resultTot.push(result);
        }
      }
      return resultTot;
    }

    function obtenerDescripPUC() {
      var intDMinReg = 0;
      var intDMaxReg = 1000;
      var DbolStop = false;
      var ArrReturn = new Array();

      var busqueda = search.create({
        type: "customrecord_lmry_co_puc",
        filters: [
          ["formulatext: LENGTH({name})", "is", "4"],
          "AND",
          ["isinactive", "is", "F"],
          "AND",
          ["formulatext: {custrecord_lmry_co_puc_subacc_of_digit1.name}", "is", paramPUC]
        ],
        columns: [
          search.createColumn({
            name: "name",
            sort: search.Sort.ASC,
            label: "0. PUC 4D"
          }),
          search.createColumn({
            name: "custrecord_lmry_co_puc",
            label: "1. DESCRIP. PUC 4D"
          }),
          search.createColumn({
            name: "formulatext",
            formula: "{custrecord_lmry_co_puc_subacc_of_digit2.custrecord_lmry_co_puc}",
            label: "2. DESCRIP PUC 2D"
          }),
          search.createColumn({
            name: "formulatext",
            formula: "{custrecord_lmry_co_puc_subacc_of_digit1.custrecord_lmry_co_puc}",
            label: "3. DESCRIP PUC 1D"
          })
        ]
      });

      var savedsearch = busqueda.run();

      while (!DbolStop) {
        var objResult = savedsearch.getRange(intDMinReg, intDMaxReg);

        if (objResult != null) {
          if (objResult.length != 1000) {
            DbolStop = true;
          }

          for (var i = 0; i < objResult.length; i++) {
            var columns = objResult[i].columns;
            var arrAuxiliar = new Array();
            //0. PUC 4D
            if (objResult[i].getValue(columns[0]) != null && objResult[i].getValue(columns[0]) != '')
              arrAuxiliar[0] = objResult[i].getValue(columns[0]);
            else
              arrAuxiliar[0] = '';
            //1. DESCRIPCION PUC 4D
            if (objResult[i].getValue(columns[1]) != null && objResult[i].getValue(columns[1]) != '')
              arrAuxiliar[1] = objResult[i].getValue(columns[1]);
            else
              arrAuxiliar[1] = '';
            //1. DESCRIPCION PUC 2D
            if (objResult[i].getValue(columns[2]) != null && objResult[i].getValue(columns[2]) != '')
              arrAuxiliar[2] = objResult[i].getValue(columns[2]);
            else
              arrAuxiliar[2] = '';
            //1. DESCRIPCION PUC 1D
            if (objResult[i].getValue(columns[3]) != null && objResult[i].getValue(columns[3]) != '')
              arrAuxiliar[3] = objResult[i].getValue(columns[3]);
            else
              arrAuxiliar[3] = '';

            ArrReturn.push(arrAuxiliar);
          }

          if (!DbolStop) {
            intDMinReg = intDMaxReg;
            intDMaxReg += 1000;
          }
        } else {
          DbolStop = true;
        }
      }

      return ArrReturn;
    }

    function llamarMapReduce() {
      var params = {};
      params['custscript_test_invbal_logid'] = paramLogId;
      params['custscript_test_invbal_periodo'] = paramPeriodo;
      params['custscript_test_invbal_fileid'] = paramFile;
      params['custscript_test_invbal_lastpuc'] = paramPUC;
      params['custscript_test_invbal_adjust'] = paramAdjustment;

      if (featSubsi) {
        params['custscript_test_invbal_subsi'] = paramSubsidi;
      }
      if (featMulti) {
        params['custscript_test_invbal_multibook'] = paramMulti;
      }

      var RedirecSchdl = task.create({
        taskType: task.TaskType.MAP_REDUCE,
        scriptId: 'customscript_test_co_inv_bal_mprd',
        deploymentId: 'customdeploy_test_co_inv_bal_mprd',
        params: params
      });
      log.debug('llamando a map reduce para PUC', params);
      RedirecSchdl.submit();
    }

    function formatear(data) {
      var strTotal = '';
      for (var i = 0; i < data.length; i++) {
        strTotal += data[i].join('|') + '\r\n';
      }
      return strTotal;
    }

    function juntarSaldoYMovimiento(arrDataSaldo, arrDataMovimiento) {
      var arrTotal = arrDataSaldo;

      for (var i = 0; i < arrTotal.length; i++) {
        var json = matchPUC(arrTotal[i][0]);
        arrTotal[i].push(json.puc4);
        arrTotal[i].push(json.puc2);
        arrTotal[i].push(json.puc1);

        var cant = arrDataMovimiento.length;
        var j = 0;
        while (j < cant) {
          if (arrTotal[i][0] == arrDataMovimiento[j][0]) {
            //log.debug('arrDataMovimiento[i][3]',arrTotal[i][3]);
            //log.debug('arrDataMovimiento[j][7]',arrDataMovimiento[j][7]);
            arrTotal[i][3] = redondear(Number(arrTotal[i][3]) + Number(arrDataMovimiento[j][7]));
            //log.debug('arrTotal[i][3]',arrTotal[i][3]);
            arrDataMovimiento.splice(j, 1);
            cant--;
            break;
          } else {
            j++;
          }
        }
      }
      //log.debug('arrTotal', arrTotal);
      //log.debug('arrDataMovimiento', arrDataMovimiento);
      for (var i = 0; i < arrDataMovimiento.length; i++) {
        var arrMovimientosN = new Array();
        arrMovimientosN.push(arrDataMovimiento[i][0]);
        arrMovimientosN.push('');
        arrMovimientosN.push('');
        arrMovimientosN.push(Number(arrDataMovimiento[i][7]));
        var json = matchPUC(arrDataMovimiento[i][0]);
        arrMovimientosN.push(json.puc4);
        arrMovimientosN.push(json.puc2);
        arrMovimientosN.push(json.puc1);
        arrTotal.push(arrMovimientosN);
      }
      arrTotal = OrdenarCuentas(arrTotal);
      return arrTotal;
    }

    function matchPUC(puc) {
      var i = 0;
      var cant = Pucs.length;
      var jsonData = {
        puc1: '',
        puc2: '',
        puc4: '',
      };
      while (i < cant) {
        if (Pucs[i][0] == puc) {
          jsonData.puc4 = Pucs[i][1];
          jsonData.puc2 = Pucs[i][2];
          jsonData.puc1 = Pucs[i][3];
          Pucs.splice(i, 1);
          break;
        } else {
          i++;
        }
      }
      return jsonData;
    }

    function obtenerDataAnterior() {
      if (paramFile != '' && paramFile != null) {
        var fileObj = fileModulo.load({
          id: paramFile
        });
        var lineas = fileObj.getContents()
        lineas = lineas.split('\r\n');

        for (var i = 0; i < lineas.length; i++) {
          var detail = lineas[i].split('|');
          //log.debug('detail',detail);
          if (detail[0] != '') {
            if (detail[0].charAt(0) == paramPUC) {
              SaldoAnteriorPUC.push(detail);
            } else {
              SaldoAnterior.push(detail);
            }
          }

        }
      } else {
        log.debug('Alerta', 'No existe parametro de archivo');
      }
    }

    function SetPUCMultibook(ArrTemp) {
      for (var i = 0; i < ArrTemp.length; i++) {
        for (var j = 0; j < ArrAccounts.length; j++) {
          if (ArrTemp[i][13] == ArrAccounts[j][0]) {
            ArrTemp[i][0] = ArrAccounts[j][6];
            ArrTemp[i][1] = ArrAccounts[j][7];
            ArrTemp[i][2] = ArrAccounts[j][4];
            ArrTemp[i][3] = ArrAccounts[j][5];
            ArrTemp[i][4] = ArrAccounts[j][2];
            ArrTemp[i][5] = ArrAccounts[j][3];
            break;
          }
        }
      }

      return ArrTemp;
    }

    function ObtenerCuentas() {
      var intDMinReg = 0;
      var intDMaxReg = 1000;
      var DbolStop = false;
      var ArrReturn = new Array();
      var cont = 0;

      var busqueda = search.create({
        type: search.Type.ACCOUNT,
        filters: [
          ['isinactive', 'is', 'F'], 'and', ['subsidiary', 'is', paramSubsidi], 'and', ['custrecord_lmry_co_puc_d4_id', 'isnotempty', '']
        ],
        columns: ['internalid', 'number', 'custrecord_lmry_co_puc_d1_id', 'custrecord_lmry_co_puc_d1_description', 'custrecord_lmry_co_puc_d2_id', 'custrecord_lmry_co_puc_d2_description', 'custrecord_lmry_co_puc_d4_id', 'custrecord_lmry_co_puc_d4_description', 'custrecord_lmry_co_puc_d6_id', 'custrecord_lmry_co_puc_d6_description']
      });

      var savedsearch = busqueda.run();

      while (!DbolStop) {
        var objResult = savedsearch.getRange(intDMinReg, intDMaxReg);

        if (objResult != null) {
          if (objResult.length != 1000) {
            DbolStop = true;
          }

          for (var i = 0; i < objResult.length; i++) {
            var columns = objResult[i].columns;
            var arrAuxiliar = new Array();
            //0. Internal Id
            if (objResult[i].getValue(columns[0]) != null && objResult[i].getValue(columns[0]) != '')
              arrAuxiliar[0] = objResult[i].getValue(columns[0]);
            else
              arrAuxiliar[0] = '';
            //1. number
            if (objResult[i].getValue(columns[1]) != null && objResult[i].getValue(columns[1]) != '')
              arrAuxiliar[1] = objResult[i].getValue(columns[1]);
            else
              arrAuxiliar[1] = '';
            //2. puc 1 id
            if (objResult[i].getText(columns[2]) != null && objResult[i].getText(columns[2]) != '')
              arrAuxiliar[2] = objResult[i].getText(columns[2]);
            else
              arrAuxiliar[2] = '';
            //3. puc 1 des
            if (objResult[i].getValue(columns[3]) != null && objResult[i].getValue(columns[3]) != '')
              arrAuxiliar[3] = objResult[i].getValue(columns[3]);
            else
              arrAuxiliar[3] = '';
            //4. puc 2 id
            if (objResult[i].getText(columns[4]) != null && objResult[i].getText(columns[4]) != '')
              arrAuxiliar[4] = objResult[i].getText(columns[4]);
            else
              arrAuxiliar[4] = '';
            //5. puc 2 des
            if (objResult[i].getValue(columns[5]) != null && objResult[i].getValue(columns[5]) != '')
              arrAuxiliar[5] = objResult[i].getValue(columns[5]);
            else
              arrAuxiliar[5] = '';
            //6. puc 4 id
            if (objResult[i].getText(columns[6]) != null && objResult[i].getText(columns[6]) != '')
              arrAuxiliar[6] = objResult[i].getText(columns[6]);
            else
              arrAuxiliar[6] = '';
            //7. puc 4 des
            if (objResult[i].getValue(columns[7]) != null && objResult[i].getValue(columns[7]) != '')
              arrAuxiliar[7] = objResult[i].getValue(columns[7]);
            else
              arrAuxiliar[7] = '';
            //8. puc 6 id
            if (objResult[i].getText(columns[8]) != null && objResult[i].getText(columns[8]) != '')
              arrAuxiliar[8] = objResult[i].getText(columns[8]);
            else
              arrAuxiliar[8] = '';
            //9. puc 6 id
            if (objResult[i].getValue(columns[9]) != null && objResult[i].getValue(columns[9]) != '')
              arrAuxiliar[9] = objResult[i].getValue(columns[9]);
            else
              arrAuxiliar[9] = '';


            ArrReturn[cont] = arrAuxiliar;
            cont++;
          }

          if (!DbolStop) {
            intDMinReg = intDMaxReg;
            intDMaxReg += 1000;
          }
        } else {
          DbolStop = true;
        }
      }

      return ArrReturn;
    }

    function AgruparCuentas(ArrTemp) {
      var ArrReturn = new Array();
      ArrReturn.push(ArrTemp[0]);

      for (var i = 1; i < ArrTemp.length; i++) {
        if (ArrTemp[i][0].trim() != '') {
          var intLength = ArrReturn.length;
          for (var j = 0; j < intLength; j++) {
            if (ArrReturn[j][0] == ArrTemp[i][0]) {
              //log.debug('ArrReturn[j][7]',ArrReturn[j][7]);
              //log.debug('ArrReturn[i][7]',ArrTemp[i][7]);
              ArrReturn[j][7] = redondear(ArrReturn[j][7] + ArrTemp[i][7]);
              //log.debug('ArrReturn[j][7] result',ArrReturn[j][7]);
              break;
            }
            if (j == ArrReturn.length - 1) {
              ArrReturn.push(ArrTemp[i]);
            }
          }
        }
      }

      // Colocar montos 0 a lineas con decimales exageradamente pequeños
      for (var iter = 0; iter < ArrReturn.length; iter++) {
        var montoRedondeado = Math.abs(redondear(ArrReturn[iter][7]));
        if (montoRedondeado == 0) {
          ArrReturn[iter][7] = 0;
        }
      }

      return ArrReturn;
    }

    function GenerarExcel(arr4D, arr2D, arr1D) {
      //Obtengo el total de Lineas a imprimir
      //var nLinea = 3000;
      xlsString = '<?xml version="1.0" encoding="UTF-8" ?><?mso-application progid="Excel.Sheet"?>';
      xlsString += '<Workbook xmlns="urn:schemas-microsoft-com:office:spreadsheet" ';
      xlsString += 'xmlns:o="urn:schemas-microsoft-com:office:office" ';
      xlsString += 'xmlns:x="urn:schemas-microsoft-com:office:excel" ';
      xlsString += 'xmlns:ss="urn:schemas-microsoft-com:office:spreadsheet" ';
      xlsString += 'xmlns:html="http://www.w3.org/TR/REC-html40">';
      xlsString += '<Styles>';
      xlsString += '<Style ss:ID="s21"><Font ss:Bold="1" ss:Size="12" /><Alignment ss:Horizontal="Center" ss:Vertical="Bottom"/></Style>';
      xlsString += '<Style ss:ID="s22"><Font ss:Bold="1"/><Alignment ss:Vertical="Bottom"/></Style>';
      xlsString += '<Style ss:ID="s23"><Font ss:Bold="1"/><Alignment ss:Vertical="Bottom"/><NumberFormat ss:Format="_(* #,###.00_);_(* \(#,###.00\);_(* &quot;-&quot;??_);_(@_)"/></Style>';
      xlsString += '<Style ss:ID="s24"><NumberFormat ss:Format="_(* #,###.00_);_(* \(#,##0.00\);_(* &quot;-&quot;??_);_(@_)"/></Style>';
      xlsString += '</Styles><Worksheet ss:Name="Sheet1">';

      xlsString += '<Table>';
      xlsString += '<Column ss:AutoFitWidth="0" ss:Width="100"/>';
      xlsString += '<Column ss:AutoFitWidth="0" ss:Width="150"/>';
      xlsString += '<Column ss:AutoFitWidth="0" ss:Width="100"/>';
      xlsString += '<Column ss:AutoFitWidth="0" ss:Width="100"/>';

      //Cabecera
      xlsString += '<Row>';
      xlsString += '<Cell></Cell>';
      xlsString += '<Cell ss:StyleID="s21"><Data ss:Type="String"> LIBRO DE INVENTARIO Y BALANCE </Data></Cell>';
      xlsString += '</Row>';
      xlsString += '<Row></Row>';
      xlsString += '<Row>';
      xlsString += '<Cell></Cell>';
      // nlapiLogExecution('ERROR', 'paramPeriodo-> ', paramPeriodo);

      xlsString += '<Cell  ss:StyleID="s22"><Data ss:Type="String">Razon Social: ' + companyname + '</Data></Cell>';
      xlsString += '</Row>';
      xlsString += '<Row>';
      xlsString += '<Cell></Cell>';
      xlsString += '<Cell  ss:StyleID="s22"><Data ss:Type="String">NIT: ' + companyruc + '</Data></Cell>';
      xlsString += '</Row>';
      xlsString += '<Row>';
      xlsString += '<Cell></Cell>';
      xlsString += '<Cell  ss:StyleID="s22"><Data ss:Type="String">Corte al: ' + Fecha_Corte_al + '</Data></Cell>';
      xlsString += '</Row>';
      if ((featMulti || featMulti == 'T') && (paramMulti != '' && paramMulti != null)) {
        xlsString += '<Row>';
        xlsString += '<Cell></Cell>';
        xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String">Multibooking: ' + multibookName + '</Data></Cell>';
        xlsString += '</Row>';
      }
      xlsString += '<Row></Row>';
      xlsString += '<Row></Row>';
      xlsString += '<Row>' +
        '<Cell ss:StyleID="s21"><Data ss:Type="String"> Cuenta </Data></Cell>' +
        '<Cell ss:StyleID="s21"><Data ss:Type="String"> Denominacion </Data></Cell>' +
        '<Cell ss:StyleID="s21"><Data ss:Type="String"> Importe </Data></Cell>' +
        '</Row>';

      var j = 0;
      var k = 0;
      for (var i = 0; i < arr1D.length; i++) {
        xlsString += '<Row>';
        xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String">' + arr1D[i][0] + '</Data></Cell>';
        xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String">' + arr1D[i][1] + '</Data></Cell>';
        xlsString += '<Cell ss:StyleID="s23"><Data ss:Type="Number">' + arr1D[i][2] + '</Data></Cell>';
        xlsString += '</Row>';

        while (j < arr2D.length) {
          if (arr2D[j][0].substring(0, 1) == arr1D[i][0]) {
            xlsString += '<Row>';
            xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String">' + arr2D[j][0] + '</Data></Cell>';
            xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String">' + arr2D[j][1] + '</Data></Cell>';
            xlsString += '<Cell ss:StyleID="s23"><Data ss:Type="Number">' + arr2D[j][2] + '</Data></Cell>';
            xlsString += '</Row>';

            while (k < arr4D.length) {
              if (arr4D[k][0].substring(0, 2) == arr2D[j][0]) {
                xlsString += '<Row>';
                xlsString += '<Cell><Data ss:Type="String">' + arr4D[k][0] + '</Data></Cell>';
                xlsString += '<Cell><Data ss:Type="String">' + arr4D[k][4] + '</Data></Cell>';
                xlsString += '<Cell ss:StyleID="s24"><Data ss:Type="Number">' + arr4D[k][3] + '</Data></Cell>';
                xlsString += '</Row>';
                k++;
              } else {
                break;
              }

            }
            j++;
          } else {
            break;
          }

        }
      }

      // CAMBIO 2016/04/14 - FILA DIFERENCIA
      // Operacion con las Cuentas de 1 Digito (ACTIVOS + GASTOS - INGRESOS - PASIVO - PATRIMONIO)
      var montoTotal1Dig = arr1D.reduce(function (contador, e) {
        return redondear(contador + e[2]);
      }, 0)
      log.debug('montoTotal1Dig', montoTotal1Dig);
      var montoTotal2Dig = arr2D.reduce(function (contador, e) {
        return redondear(contador + e[2]);
      }, 0)
      log.debug('montoTotal2Dig', montoTotal2Dig);

      xlsString += '<Row>';
      //xlsString += '<Cell></Cell>';
      xlsString += '<Cell></Cell>';
      xlsString += '<Cell  ss:StyleID="s22"><Data ss:Type="String">DIFERENCIA</Data></Cell>';
      xlsString += '<Cell ss:StyleID="s23"><Data ss:Type="Number">' + (montoTotal1Dig - montoTotal2Dig) + '</Data></Cell>';
      xlsString += '</Row>';
      xlsString += '</Table></Worksheet></Workbook>';

      // Se arma el archivo EXCEL
      Final_string = encode.convert({
        string: xlsString,
        inputEncoding: encode.Encoding.UTF_8,
        outputEncoding: encode.Encoding.BASE_64
      });

      var nameFile = Name_File();
      saveFile(Final_string, nameFile, 'xls');
    }

    function ValidatePrimaryBook() {
      var accbook_check = search.lookupFields({
        type: search.Type.ACCOUNTING_BOOK,
        id: paramMulti,
        columns: ['isprimary']
      });

      return accbook_check.isprimary;
    }

    function redondear(number) {
      return Math.round(Number(number) * 100) / 100;
    }

    function ObtieneSpecificTransaction() {
      // Control de Memoria
      var intDMaxReg = 1000;
      var intDMinReg = 0;
      var arrQuiebre = new Array();
      // Exedio las unidades
      var DbolStop = false;
      var arrCuatroDigitos = new Array();
      var _contg = 0;
      //var _cont = 0;

      var savedsearch = search.load({
        /*LatamReady - CO Inventory Book and Balance L.EspecMulti*/
        id: 'customsearch_lmry_co_invent_balanc_liesp'
      });

      var pucFilter = search.createFilter({
        name: 'formulatext',
        formula: '{account.custrecord_lmry_co_puc_d4_id}',
        operator: search.Operator.STARTSWITH,
        values: [paramPUC]
      });
      savedsearch.filters.push(pucFilter);

      if (featSubsi) {
        var subsidiaryFilter = search.createFilter({
          name: 'subsidiary',
          operator: search.Operator.IS,
          values: [paramSubsidi]
        });
        savedsearch.filters.push(subsidiaryFilter);
      }

      var periodosSTR = PeriodosRestantes.toString();
      var periodFilterFROM = search.createFilter({
        name: 'formulanumeric',
        formula: 'CASE WHEN {transaction.postingperiod.id} IN (' + periodosSTR + ') THEN 1 ELSE 0 END',
        operator: search.Operator.EQUALTO,
        values: [1]
      });
      savedsearch.filters.push(periodFilterFROM);

      var multibookFilter = search.createFilter({
        name: 'accountingbook',
        // join: 'accountingtransaction',
        operator: search.Operator.IS,
        values: [paramMulti]
      });
      savedsearch.filters.push(multibookFilter);

      var searchresult = savedsearch.run();

      while (!DbolStop) {
        var objResult = searchresult.getRange(intDMinReg, intDMaxReg);

        if (objResult != null) {
          var intLength = objResult.length;

          if (intLength != 1000) {
            DbolStop = true;
          }

          for (var i = 0; i < intLength; i++) {
            var columns = objResult[i].columns;
            arrQuiebre = new Array();

            for (var col = 0; col < columns.length; col++) {
              if (col == 8) {
                arrQuiebre[col] = objResult[i].getText(columns[col]);
              } else if (col == 7) {
                arrQuiebre[col] = redondear(objResult[i].getValue(columns[col]) * objResult[i].getValue(columns[10]));
              } else {
                arrQuiebre[col] = objResult[i].getValue(columns[col]);
              }
            }

            if (arrQuiebre[7] != 0) {
              arrCuatroDigitos[_contg] = arrQuiebre;
              _contg++;
            }

          }

          intDMinReg = intDMaxReg;
          intDMaxReg += 1000;
          if (intLength < 1000) {
            DbolStop = true;
          }
        } else {
          DbolStop = true;
        }
      }

      return arrCuatroDigitos;
    }

    function ObtieneTransacciones() {
      // Control de Memoria
      var intDMaxReg = 1000;
      var intDMinReg = 0;
      // Exedio las unidades
      var DbolStop = false;
      var arrCuatroDigitos = new Array();
      var _contg = 0;
      //var _cont = 0;

      var savedsearch = search.load({
        /*LatamReady - CO Inventory Book and Balance with L.Espec*/
        id: 'customsearch_lmry_co_invent_balanc_trale'
      });

      var pucFilter = search.createFilter({
        name: 'formulatext',
        formula: '{account.custrecord_lmry_co_puc_d4_id}',
        operator: search.Operator.STARTSWITH,
        values: [paramPUC]
      });
      savedsearch.filters.push(pucFilter);

      if (featSubsi) {
        var subsidiaryFilter = search.createFilter({
          name: 'subsidiary',
          operator: search.Operator.IS,
          values: [paramSubsidi]
        });
        savedsearch.filters.push(subsidiaryFilter);
      }

      var periodosSTR = PeriodosRestantes.toString();
      var periodFilterFROM = search.createFilter({
        name: 'formulanumeric',
        formula: 'CASE WHEN {postingperiod.id} IN (' + periodosSTR + ') THEN 1 ELSE 0 END',
        operator: search.Operator.EQUALTO,
        values: [1]
      });
      savedsearch.filters.push(periodFilterFROM);

      if (featMulti) {
        var multibookFilter = search.createFilter({
          name: 'accountingbook',
          join: 'accountingtransaction',
          operator: search.Operator.IS,
          values: [paramMulti]
        });
        savedsearch.filters.push(multibookFilter);

        var bookspecificFitler = search.createFilter({
          name: 'bookspecifictransaction',
          operator: search.Operator.IS,
          values: ['F']
        });
        savedsearch.filters.push(bookspecificFitler);
        //11.
        var exchangerateColum = search.createColumn({
          name: 'formulacurrency',
          summary: "GROUP",
          formula: "{accountingtransaction.exchangerate}"
        });
        savedsearch.columns.push(exchangerateColum);
        //12.
        var balanceColumn = search.createColumn({
          name: 'formulacurrency',
          summary: "SUM",
          formula: "NVL({accountingtransaction.debitamount},0) - NVL({accountingtransaction.creditamount},0)"
        });
        savedsearch.columns.push(balanceColumn);
        //13.
        var multiAccountColumn = search.createColumn({
          name: 'account',
          join: 'accountingtransaction',
          summary: "GROUP"
        });
        savedsearch.columns.push(multiAccountColumn);
      }

      var searchresult = savedsearch.run();

      while (!DbolStop) {
        var objResult = searchresult.getRange(intDMinReg, intDMaxReg);

        if (objResult != null) {
          var intLength = objResult.length;

          if (intLength != 1000) {
            DbolStop = true;
          }

          for (var i = 0; i < intLength; i++) {
            var columns = objResult[i].columns;
            var arrQuiebre = new Array();

            for (var col = 0; col < columns.length; col++) {
              if (col == 7) {
                if (featMulti) {
                  arrQuiebre[col] = redondear(objResult[i].getValue(columns[12]));
                } else {
                  arrQuiebre[col] = redondear(objResult[i].getValue(columns[7]));
                }
              } else {
                arrQuiebre[col] = objResult[i].getValue(columns[col]);
              }
            }

            if (arrQuiebre[7] != 0) {
              arrCuatroDigitos[_contg] = arrQuiebre;
              _contg++;
            }
          }
          intDMinReg = intDMaxReg;
          intDMaxReg += 1000;
          if (intLength < 1000) {
            DbolStop = true;
          }
        } else {
          DbolStop = true;
        }
      }

      return arrCuatroDigitos;
    }

    function ObtieneAccountingContext() {
      // Control de Memoria
      var intDMaxReg = 1000;
      var intDMinReg = 0;
      var arrAuxiliar = new Array();
      var contador_auxiliar = 0;

      var DbolStop = false;

      var savedsearch = search.load({
        id: 'customsearch_lmry_account_context'
      });

      // Valida si es OneWorld
      if (featSubsi == true) {
        var subsidiaryFilter = search.createFilter({
          name: 'subsidiary',
          operator: search.Operator.IS,
          values: [paramSubsidi]
        });
        savedsearch.filters.push(subsidiaryFilter);
      }

      var puc4IdColumn = search.createColumn({
        name: 'formulatext',
        formula: '{custrecord_lmry_co_puc_d4_id}',
        summary: 'GROUP'
      });
      savedsearch.columns.push(puc4IdColumn);

      var puc4DesColumn = search.createColumn({
        name: 'formulatext',
        formula: '{custrecord_lmry_co_puc_d4_description}',
        summary: 'GROUP'
      });
      savedsearch.columns.push(puc4DesColumn);

      var puc2IdColumn = search.createColumn({
        name: 'formulatext',
        formula: '{custrecord_lmry_co_puc_d2_id}',
        summary: 'GROUP'
      });
      savedsearch.columns.push(puc2IdColumn);

      var puc2DesColumn = search.createColumn({
        name: 'formulatext',
        formula: '{custrecord_lmry_co_puc_d2_description}',
        summary: 'GROUP'
      });
      savedsearch.columns.push(puc2DesColumn);

      var puc1IdColumn = search.createColumn({
        name: 'formulatext',
        formula: '{custrecord_lmry_co_puc_d1_id}',
        summary: 'GROUP'
      });
      savedsearch.columns.push(puc1IdColumn);

      var puc1DesColumn = search.createColumn({
        name: 'formulatext',
        formula: '{custrecord_lmry_co_puc_d1_description}',
        summary: 'GROUP'
      });
      savedsearch.columns.push(puc1DesColumn);

      var searchresult = savedsearch.run();

      while (!DbolStop) {
        var objResult = searchresult.getRange(intDMinReg, intDMaxReg);

        if (objResult != null) {
          var intLength = objResult.length;

          if (intLength == 0) {
            DbolStop = true;
          } else {
            for (var i = 0; i < intLength; i++) {
              // Cantidad de columnas de la busqueda
              var columns = objResult[i].columns;
              arrAuxiliar = new Array();
              for (var col = 0; col < columns.length; col++) {
                if (col == 3) {
                  arrAuxiliar[col] = objResult[i].getText(columns[col]);
                } else {
                  arrAuxiliar[col] = objResult[i].getValue(columns[col]);
                }
              }

              if (arrAuxiliar[3] == multibookName) {
                arrAccountingContext[contador_auxiliar] = arrAuxiliar;
                contador_auxiliar++;
              }
            }
          }

          intDMinReg = intDMaxReg;
          intDMaxReg += 1000;

        } else {
          DbolStop = true;
        }
      }
    }

    function obtenerCuenta(numero_cuenta) {

      for (var i = 0; i < arrAccountingContext.length; i++) {
        if (numero_cuenta == arrAccountingContext[i][0]) {
          var number_cta_aux = arrAccountingContext[i][1];
          for (var j = 0; j < arrAccountingContext.length; j++) {

            if (number_cta_aux == arrAccountingContext[j][0]) {
              var ArrPuc = new Array();

              ArrPuc[0] = arrAccountingContext[j][4];
              ArrPuc[1] = arrAccountingContext[j][5];
              ArrPuc[2] = arrAccountingContext[j][6];
              ArrPuc[3] = arrAccountingContext[j][7];
              ArrPuc[4] = arrAccountingContext[j][8];
              ArrPuc[5] = arrAccountingContext[j][9];

              return ArrPuc;
            }
          }
        }
      }
      return numero_cuenta;
    }

    function CambioDeCuentas(arrCuatroDigitos) {
      for (var i = 0; i < arrCuatroDigitos.length; i++) {
        if (arrCuatroDigitos[i][8] == 'Bank' || arrCuatroDigitos[i][8] == 'Accounts Payable' || arrCuatroDigitos[i][8] == 'Accounts Receivable' ||
          arrCuatroDigitos[i][8] == 'Banco' || arrCuatroDigitos[i][8] == 'Cuentas a pagar' || arrCuatroDigitos[i][8] == 'Cuentas a cobrar') {
          var cuenta_act = obtenerCuenta(arrCuatroDigitos[i][9]);

          if (cuenta_act != arrCuatroDigitos[i][9]) {
            arrCuatroDigitos[i][0] = cuenta_act[0];
            arrCuatroDigitos[i][1] = cuenta_act[1];
            arrCuatroDigitos[i][2] = cuenta_act[2];
            arrCuatroDigitos[i][3] = cuenta_act[3];
            arrCuatroDigitos[i][4] = cuenta_act[4];
            arrCuatroDigitos[i][5] = cuenta_act[5];
          }
        }
      }
    }

    function OrdenarCuentas(arrCuatroDigitos) {
      arrCuatroDigitos.sort(sortFunction);

      function sortFunction(a, b) {
        if (a[0] === b[0]) {
          return 0;
        } else {
          return (a[0] < b[0]) ? -1 : 1;
        }
      }

      return arrCuatroDigitos;
    }


    function Name_File() {
      var _NameFile = '';

      var fecha_format = format.parse({
        value: periodenddate,
        type: format.Type.DATE
      });

      var MM = fecha_format.getMonth() + 1;
      var YYYY = fecha_format.getFullYear();
      // var DD = fecha_format.getDate();
      if (('' + MM).length == 1) {
        MM = '0' + MM;
      }
      //var  MesConvertido = Periodo(MM);
      _NameFile = "COLibroInventarioBalance" + '_' + 1 + '_' + companyname + '_' + MM + '_' + YYYY;
      return _NameFile;
    }

    function saveFile(data, nameFile, extension) {
      // Ruta de la carpeta contenedora
      var FolderId = objContext.getParameter({
        name: 'custscript_lmry_file_cabinet_rg_co'
      });
      // Almacena en la carpeta de Archivos Generados
      if (FolderId != '' && FolderId != null) {
        // Crea el archivo.xls
        if (extension == 'txt') {
          var file = fileModulo.create({
            name: nameFile + '.' + extension,
            fileType: fileModulo.Type.PLAINTEXT,
            contents: data,
            folder: FolderId
          });
        } else {
          var file = fileModulo.create({
            name: nameFile + '.' + extension,
            fileType: fileModulo.Type.EXCEL,
            contents: data,
            folder: FolderId
          });
        }
        // Termina de grabar el archivo
        var idfile = file.save();
        log.debug('Se actualizo archivo temporal con id: ', idfile);
        // Trae URL de archivo generado
        if (extension == 'xls') {
          var idfile2 = fileModulo.load({
            id: idfile
          });
          // Obtenemos de las prefencias generales el URL de Netsuite (Produccion o Sandbox)
          var getURL = objContext.getParameter({
            name: 'custscript_lmry_netsuite_location'
          });

          var urlfile = '';
          if (getURL != '' && getURL != '') {
            urlfile += 'https://' + getURL;
          }
          urlfile += idfile2.url;

          //Genera registro personalizado como log
          if (idfile) {
            var usuarioTemp = runtime.getCurrentUser();
            var id = usuarioTemp.id;
            var employeename = search.lookupFields({
              type: search.Type.EMPLOYEE,
              id: id,
              columns: ['firstname', 'lastname']
            });
            var usuario = employeename.firstname + ' ' + employeename.lastname;

            var record = recordModulo.load({
              type: 'customrecord_lmry_co_rpt_generator_log',
              id: paramLogId
            });
            //Nombre de Archivo
            record.setValue({
              fieldId: 'custrecord_lmry_co_rg_name',
              value: nameFile + '.' + extension
            });
            //Periodo
            record.setValue({
              fieldId: 'custrecord_lmry_co_rg_postingperiod',
              value: periodname
            });
            //Nombre de Reporte
            record.setValue({
              fieldId: 'custrecord_lmry_co_rg_transaction',
              value: 'CO - Libro de Inventario y Balance 2.0'
            });
            //Nombre de Subsidiaria
            record.setValue({
              fieldId: 'custrecord_lmry_co_rg_subsidiary',
              value: companyname
            });
            //Url de Archivo
            record.setValue({
              fieldId: 'custrecord_lmry_co_rg_url_file',
              value: urlfile
            });
            //Multibook
            if (featMulti || featMulti == 'T') {
              record.setValue({
                fieldId: 'custrecord_lmry_co_rg_multibook',
                value: multibookName
              });
            }
            //Creado Por
            record.setValue({
              fieldId: 'custrecord_lmry_co_rg_employee',
              value: usuario
            });

            var recordId = record.save();
            // Envia mail de conformidad al usuario
            libreria.sendrptuser('CO - Libro de Inventario y Balance 2.0', 3, nameFile);
          }
        }

      } else {
        // Debug
        log.error({
          title: 'DEBUG',
          details: 'Creacion de Txt' +
            'No se existe el folder'
        });
      }
    }

    function RecordNoData() {

      var usuarioTemp = runtime.getCurrentUser();

      var id = usuarioTemp.id;
      var employeename = search.lookupFields({
        type: search.Type.EMPLOYEE,
        id: id,
        columns: ['firstname', 'lastname']
      });
      var usuario = employeename.firstname + ' ' + employeename.lastname;

      var record = recordModulo.load({
        type: 'customrecord_lmry_co_rpt_generator_log',
        id: paramLogId
      });
      //Nombre de Archivo
      record.setValue({
        fieldId: 'custrecord_lmry_co_rg_name',
        value: 'No existe informacion para los criterios seleccionados.'
      });
      //Nombre de Reporte
      record.setValue({
        fieldId: 'custrecord_lmry_co_rg_transaction',
        value: 'CO - Libro de Inventario y Balance 2.0'
      });
      //Nombre de Subsidiaria
      record.setValue({
        fieldId: 'custrecord_lmry_co_rg_subsidiary',
        value: companyname
      });
      //Periodo
      record.setValue({
        fieldId: 'custrecord_lmry_co_rg_postingperiod',
        value: periodname
      });
      //Multibook
      if (featMulti) {
        record.setValue({
          fieldId: 'custrecord_lmry_co_rg_multibook',
          value: multibookName
        });
      }
      //Creado Por
      record.setValue({
        fieldId: 'custrecord_lmry_co_rg_employee',
        value: usuario
      });

      var recordId = record.save();
    }

    function ObtenerDatosSubsidiaria() {

      var configpage = config.load({
        type: config.Type.COMPANY_INFORMATION
      });
      if (featSubsi) {
        companyname = ObtainNameSubsidiaria(paramSubsidi);
        companyruc = ObtainFederalIdSubsidiaria(paramSubsidi);

      } else {
        companyruc = configpage.getFieldValue('employerid');
        companyname = configpage.getFieldValue('legalname');

      }
      companyruc = companyruc.replace(' ', '');
      companyname = companyname.replace(' ', '');

    }

    function ObtainNameSubsidiaria(subsidiary) {
      try {
        if (subsidiary != '' && subsidiary != null) {
          var subsidyName = search.lookupFields({
            type: search.Type.SUBSIDIARY,
            id: subsidiary,
            columns: ['legalname']
          });
          return subsidyName.legalname
        }
      } catch (err) {
        libreria.sendMail(LMRY_script, ' [ ObtainNameSubsidiaria ] ' + err);
      }
      return '';
    }

    function ObtainFederalIdSubsidiaria(subsidiary) {
      try {
        if (subsidiary != '' && subsidiary != null) {
          var federalId = search.lookupFields({
            type: search.Type.SUBSIDIARY,
            id: subsidiary,
            columns: ['taxidnum']
          });
          return federalId.taxidnum
        }
      } catch (err) {
        libreria.sendMail(LMRY_script, ' [ ObtainFederalIdSubsidiaria ] ' + err);
      }
      return '';
    }

    //-------------------------------------------------------------------------------------------------------
    //Obtiene a?o y mes del periodo
    //-------------------------------------------------------------------------------------------------------
    function Periodo(periodo) {
      periodo = periodo + '';

      var auxmess = '';
      switch (periodo) {

        case '01':
          auxmess = 'Ene';
          break;
        case '02':
          auxmess = 'Feb';
          break;
        case '03':
          auxmess = 'Mar';
          break;
        case '04':
          auxmess = 'Abr';
          break;
        case '05':
          auxmess = 'May';
          break;
        case '06':
          auxmess = 'Jun';
          break;
        case '07':
          auxmess = 'Jul';
          break;
        case '08':
          auxmess = 'Ago';
          break;
        case '09':
          auxmess = 'Set';
          break;
        case '10':
          auxmess = 'Oct';
          break;
        case '11':
          auxmess = 'Nov';
          break;
        case '12':
          auxmess = 'Dic';
          break;
      }

      return auxmess;
    }

    function ObtenerParametrosYFeatures() {
      //Parametros
      paramSubsidi = objContext.getParameter({
        name: 'custscript_test_co_invbalv2_subsi'
      });
      paramPeriodo = objContext.getParameter({
        name: 'custscript_test_co_invbalv2_periodo'
      });
      paramPeriodsRestantes = objContext.getParameter({
        name: 'custscript_test_co_invbalv2_period_res'
      });
      paramMulti = objContext.getParameter({
        name: 'custscript_test_co_invbalv2_multibook'
      });
      paramLogId = objContext.getParameter({
        name: 'custscript_test_co_invbalv2_logid'
      });
      paramPUC = objContext.getParameter({
        name: 'custscript_test_co_invbalv2_puc' //primer digito
      });
      paramFile = objContext.getParameter({
        name: 'custscript_test_co_invbalv2_fileid'
      });
      paramAdjustment = objContext.getParameter({
        name: 'custscript_test_co_invbalv2_adjust'
      });
      //Features
      featSubsi = runtime.isFeatureInEffect({
        feature: "SUBSIDIARIES"
      });
      featMulti = runtime.isFeatureInEffect({
        feature: "MULTIBOOK"
      });

      log.error({
        title: 'ENTROfeats',
        details: paramLogId + ' ' + paramMulti + ' ' + paramSubsidi + ' ' + paramPeriodo + ' ' + paramPeriodsRestantes + ' ' + paramPUC + ' ' + paramFile
      });


      //Period enddate para el nombre del libro
      var period_temp = search.lookupFields({
        type: search.Type.ACCOUNTING_PERIOD,
        id: paramPeriodo,
        columns: ['periodname', 'startdate', 'enddate']
      });

      periodenddate = period_temp.enddate;
      periodstartdate = period_temp.startdate;
      periodname = period_temp.periodname;

      language = runtime.getCurrentScript().getParameter("LANGUAGE").substring(0, 2);

      var fecha_format = format.parse({
        value: periodstartdate,
        type: format.Type.DATE
      });

      var MM = fecha_format.getMonth() + 1;
      var YYYY = fecha_format.getFullYear();

      if (('' + MM).length == 1) {
        MM = '0' + MM;
      }
      if (language == 'es') {
        //Obtener mes en español
        Fecha_Corte_al = Periodo(MM) + ' ' + YYYY;
      } else {
        Fecha_Corte_al = periodname;
      }

      //Multibook Name
      if (featMulti) {
        var multibookName_temp = search.lookupFields({
          type: search.Type.ACCOUNTING_BOOK,
          id: paramMulti,
          columns: ['name']
        });

        multibookName = multibookName_temp.name;
      }

      var result_f_temp = search.create({
        type: search.Type.CURRENCY,
        columns: ['name', 'symbol']
      });
      var result_f_temp2 = result_f_temp.run();
      result_f = result_f_temp2.getRange(0, 1000);
    }

    return {
      execute: execute
    };
  });
