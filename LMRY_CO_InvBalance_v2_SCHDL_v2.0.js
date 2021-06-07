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
define(["N/record", "N/runtime", "N/file", "N/email", "N/encode", "N/search",
    "N/format", "N/log", "N/config", "N/sftp", "./CO_Library_Mensual/LMRY_CO_Reportes_LBRY_V2.0.js", "N/task", "N/render"
  ],

  function(recordModulo, runtime, fileModulo, email, encode, search, format, log,
    config, sftp, libreria, task, render) {
    var objContext = runtime.getCurrentScript();

    //Tamaño
    var file_size = 7340032;

    // Nombre del Reporte
    var namereport = "Reportes Libro de Inventario y Balance";
    var LMRY_script = 'LMRY CO Reportes Libro de Inventario y Balance SCHDL 2.0';

    //Parametros
    var paramsubsidi = '';
    var paramperiodo = '';
    var paramidrpt = '';
    var paramMulti = '';
    var paramCont = '';
    var paramBucle = '';

    //Control de Reporte
    var periodstartdate = '';
    var periodenddate = '';
    var antperiodenddate = '';
    var companyruc = '';
    var companyname = '';

    var xlsString = '';

    var ArrMontosAntesSpecific = new Array();
    var ArrSaldoAnterior = new Array();
    var arrAccountingContext = new Array();
    var ArrAccounts = new Array();

    var montoTotal1Dig = 0;
    var montoTotal2Dig = 0;
    var strName = '';
    var periodname = '';
    var auxmess = '';
    var auxanio = '';

    var Final_string;
    var ip;

    var multibookName = '';
    var ArrPeriodos = new Array();

    var language;
    var Fecha_Corte_al;

    /* ***********************************************
     * Arreglo con la structura de la tabla log
     * ******************************************** */
    var RecordName = 'customrecord_lmry_co_rpt_generator_log';
    var RecordTable = ['custrecord_lmry_co_rg_name',
      'custrecord_lmry_co_rg_postingperiod',
      'custrecord_lmry_co_rg_subsidiary',
      'custrecord_lmry_co_rg_url_file',
      'custrecord_lmry_co_rg_employee',
      'custrecord_lmry_co_rg_multibook'
    ];

    //Features
    var featSubsi = null;
    var featMulti = null;

    //var featuremultib = objContext.getFeature('MULTIBOOKMULTICURR');

    var result_f;

    function execute(context) {

      try {
        ObtenerParametrosYFeatures();

        ObtenerDatosSubsidiaria();

        if (featMulti) {
          ArrAccounts = ObtenerCuentas();

          if (!ValidatePrimaryBook() || ValidatePrimaryBook() != 'T') {
            var array_context = ObtieneAccountingContext();
          }
        }

        ArrSaldoAnterior = ObtieneTransacciones();
        if (featMulti) {
          ArrSaldoAnterior = SetPUCMultibook(ArrSaldoAnterior);
        }

        if (featMulti) {
          ArrMontosAntesSpecific = ObtieneSpecificTransaction();

          if (ArrMontosAntesSpecific.length != 0) {
            JuntarArreglosSpecificos(ArrSaldoAnterior, ArrMontosAntesSpecific);
          }
        }


        if (featMulti) {
          if (!ValidatePrimaryBook() || ValidatePrimaryBook() != 'T') {
            CambioDeCuentas(ArrSaldoAnterior);
          }
        }

        if (ArrSaldoAnterior.length != 0) {

          if (ArrSaldoAnterior.length > 1) {
            ArrSaldoAnterior = OrdenarCuentas(ArrSaldoAnterior);
            //log.debug('Ordenar cuentas',ArrSaldoAnterior);
            ArrSaldoAnterior = AgruparCuentas(ArrSaldoAnterior);
          }

          LlenarArregloDosDigitos(ArrSaldoAnterior);

          LlenarArregloUnDigitos(ArrSaldoAnterior);
        }

        GenerarExcel(ArrSaldoAnterior);


      } catch (err) {
        libreria.sendMail(LMRY_script, ' [ execute ] ' + err);
        //var varMsgError = 'No se pudo procesar el Schedule.';

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

      var infoTxt = '';
      var DbolStop = false;

      var ArrReturn = new Array();
      var cont = 0;

      var busqueda = search.create({
        type: search.Type.ACCOUNT,
        filters: [
          ['isinactive', 'is', 'F'], 'and', ['subsidiary', 'is', paramsubsidi], 'and', ['custrecord_lmry_co_puc_d4_id', 'isnotempty', '']
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
              ArrReturn[j][7] = Number(ArrReturn[j][7]) + Number(ArrTemp[i][7]);
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

    function GenerarExcel(arrCuatroDigitos) {
      //Obtengo el total de Lineas a imprimir
      var nLinea = 3000;
      var xlsLineas = arrCuatroDigitos.length;
      var NroLineas = xlsLineas / nLinea;

      if ((xlsLineas % nLinea) > 0) {
        NroLineas = NroLineas + 1;
      }
      if (arrCuatroDigitos.length != null && arrCuatroDigitos.length != 0) {

        for (ip = 1; ip <= NroLineas; ip++) {
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
          // nlapiLogExecution('ERROR', 'paramperiodo-> ', paramperiodo);

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

          for (var i = 0; i < arrCuatroDigitos.length; i++) {
            if (arrCuatroDigitos[i][0].length == 4 && Number((arrCuatroDigitos[i][0]).charAt(0)) <= 10) {
              xlsString += '<Row>';
              xlsString += '<Cell><Data ss:Type="String">' + arrCuatroDigitos[i][0] + '</Data></Cell>';
              xlsString += '<Cell><Data ss:Type="String">' + arrCuatroDigitos[i][1] + '</Data></Cell>';
              xlsString += '<Cell ss:StyleID="s24"><Data ss:Type="Number">' + arrCuatroDigitos[i][7] + '</Data></Cell>';
              // nlapiLogExecution('ERROR', 'arrCuatroDigitos_1', arrCuatroDigitos[i][0].length);
              xlsString += '</Row>';
            } else {
              if (Number((arrCuatroDigitos[i][0]).charAt(0)) <= 10) {
                xlsString += '<Row>';
                xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String">' + arrCuatroDigitos[i][0] + '</Data></Cell>';
                xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String">' + arrCuatroDigitos[i][1] + '</Data></Cell>';
                /*if (arrCuatroDigitos[i][0].length == 1){
                    xlsString += '<Cell><Data ss:Type="String"></Data></Cell>';
                }*/
                xlsString += '<Cell ss:StyleID="s23"><Data ss:Type="Number">' + arrCuatroDigitos[i][7] + '</Data></Cell>';
                // nlapiLogExecution('ERROR', 'arrCuatroDigitos_2', arrCuatroDigitos[i][0].length);
                xlsString += '</Row>';
              }
            }
          }

          // CAMBIO 2016/04/14 - FILA DIFERENCIA
          // Operacion con las Cuentas de 1 Digito (ACTIVOS + GASTOS - INGRESOS - PASIVO - PATRIMONIO)
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

          SaveFile();
        }
      } else {
        RecordNoData();

      }

    }

    function ValidatePrimaryBook() {
      var accbook_check = search.lookupFields({
        type: search.Type.ACCOUNTING_BOOK,
        id: paramMulti,
        columns: ['isprimary']
      });

      return accbook_check.isprimary;
    }

    function LlenarArregloDosDigitos(arrCuatroDigitos) {
      var aux_id_2_digitos = arrCuatroDigitos[0][2];

      var primerArrAux = new Array();
      primerArrAux[0] = arrCuatroDigitos[0][2];
      primerArrAux[1] = arrCuatroDigitos[0][3];
      primerArrAux[2] = arrCuatroDigitos[0][2];
      primerArrAux[3] = arrCuatroDigitos[0][3];
      primerArrAux[4] = arrCuatroDigitos[0][4];
      primerArrAux[5] = arrCuatroDigitos[0][5];
      primerArrAux[6] = arrCuatroDigitos[0][6];
      //  primerArrAux[7] = arrCuatroDigitos[i][7];
      primerArrAux[7] = 0.0;
      primerArrAux[8] = arrCuatroDigitos[0][8];
      primerArrAux[9] = arrCuatroDigitos[0][9];


      arrCuatroDigitos.splice(0, 0, primerArrAux);

      var arr_2_digitos = new Array();

      var cont = 0;

      arr_2_digitos[cont] = primerArrAux;
      cont++;

      for (var i = 0; i < arrCuatroDigitos.length; i++) {
        if (aux_id_2_digitos != arrCuatroDigitos[i][2]) {
          aux_id_2_digitos = arrCuatroDigitos[i][2];

          var arr = new Array();

          arr[0] = arrCuatroDigitos[i][2];
          arr[1] = arrCuatroDigitos[i][3];
          arr[2] = arrCuatroDigitos[i][2];
          arr[3] = arrCuatroDigitos[i][3];
          arr[4] = arrCuatroDigitos[i][4];
          arr[5] = arrCuatroDigitos[i][5];
          arr[6] = arrCuatroDigitos[i][6];
          arr[7] = 0.0;
          arr[8] = arrCuatroDigitos[i][8];
          arr[9] = arrCuatroDigitos[i][9];


          arrCuatroDigitos.splice(i, 0, arr);

          arr_2_digitos[cont] = arr;
          cont++;
        }
      }
      for (var i = 0; i < arr_2_digitos.length; i++) {
        for (var j = 0; j < arrCuatroDigitos.length; j++) {
          if (arr_2_digitos[i][0] == arrCuatroDigitos[j][2]) {
            arr_2_digitos[i][7] += Number(arrCuatroDigitos[j][7]);
          }
        }
      }
      //log.debug('arr_2_digitos',arr_2_digitos);
      for (var i = 0; i < arr_2_digitos.length; i++) {
        for (var j = 0; j < arrCuatroDigitos.length; j++) {
          if (arrCuatroDigitos[j][0] == arr_2_digitos[i][0]) {
            arrCuatroDigitos[j][7] = arr_2_digitos[i][7];
          }
        }
        //Se acumula monto de cuentas hijas validando que los numeros de cuenta no sean nulos.
        if (arr_2_digitos[i][0] != "" && arr_2_digitos[i][0] != "- None -" && arr_2_digitos[i][0] != null) {
          montoTotal2Dig += arr_2_digitos[i][7];
        }
      }
      montoTotal2Dig = redondear(montoTotal2Dig);
    }

    function redondear(number) {
      return Math.round(Number(number) * 100) / 100;
    }

    function LlenarArregloUnDigitos(arrCuatroDigitos) {
      var aux_id_1_digitos = arrCuatroDigitos[0][4];

      var primerArrAux = new Array();
      primerArrAux[0] = arrCuatroDigitos[0][4];
      primerArrAux[1] = arrCuatroDigitos[0][5];
      primerArrAux[2] = arrCuatroDigitos[0][2];
      primerArrAux[3] = arrCuatroDigitos[0][3];
      primerArrAux[4] = arrCuatroDigitos[0][4];
      primerArrAux[5] = arrCuatroDigitos[0][5];
      primerArrAux[6] = arrCuatroDigitos[0][6];
      primerArrAux[7] = 0.0;
      primerArrAux[8] = arrCuatroDigitos[0][8];
      primerArrAux[9] = arrCuatroDigitos[0][9];


      arrCuatroDigitos.splice(0, 0, primerArrAux);

      var arr_1_digitos = new Array();

      var cont = 0;

      arr_1_digitos[cont] = primerArrAux;
      cont++;
      for (var i = 0; i < arrCuatroDigitos.length; i++) {
        if (aux_id_1_digitos != arrCuatroDigitos[i][4]) {
          aux_id_1_digitos = arrCuatroDigitos[i][4];

          var arr = new Array();

          arr[0] = arrCuatroDigitos[i][4];
          arr[1] = arrCuatroDigitos[i][5];
          arr[2] = arrCuatroDigitos[i][2];
          arr[3] = arrCuatroDigitos[i][3];
          arr[4] = arrCuatroDigitos[i][4];
          arr[5] = arrCuatroDigitos[i][5];
          arr[6] = arrCuatroDigitos[i][6];
          arr[7] = 0.0;
          arr[8] = arrCuatroDigitos[i][8];
          arr[9] = arrCuatroDigitos[i][9];


          arrCuatroDigitos.splice(i, 0, arr);

          arr_1_digitos[cont] = arr;
          cont++;
        }
      }

      for (var i = 0; i < arr_1_digitos.length; i++) {
        for (var j = 0; j < arrCuatroDigitos.length; j++) {
          if ((arr_1_digitos[i][0] == arrCuatroDigitos[j][4]) && arrCuatroDigitos[j][0].length == 2) {
            arr_1_digitos[i][7] += Number(arrCuatroDigitos[j][7]);
          }
        }
      }
      //log.debug('arr_1_digitos',arr_1_digitos);
      for (var i = 0; i < arr_1_digitos.length; i++) {
        for (var j = 0; j < arrCuatroDigitos.length; j++) {
          if (arrCuatroDigitos[j][0] == arr_1_digitos[i][0]) {
            arrCuatroDigitos[j][7] = arr_1_digitos[i][7];
          }
        }
        montoTotal1Dig += arr_1_digitos[i][7];
      }
      montoTotal1Dig = redondear(montoTotal1Dig);
    }

    function JuntarArreglosSpecificos(arrPadre, arrHijos) {
      for (var i = 0; i < ArrMontosAntesSpecific.length; i++) {
        ArrSaldoAnterior.push(ArrMontosAntesSpecific[i]);
      }
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
        id: 'customsearch_lmry_co_invent_balanc_liesp'
      });

      if (featSubsi) {
        var subsidiaryFilter = search.createFilter({
          name: 'subsidiary',
          operator: search.Operator.IS,
          values: [paramsubsidi]
        });
        savedsearch.filters.push(subsidiaryFilter);
      }

      var periodFields = search.lookupFields({
        type: search.Type.ACCOUNTING_PERIOD,
        id: paramperiodo,
        columns: ['enddate']
      });

      var periodEndDate = periodFields.enddate;

      var periodFilter = search.createFilter({
        name: 'trandate',
        join: 'transaction',
        operator: search.Operator.ONORBEFORE,
        values: [periodEndDate]
      });
      savedsearch.filters.push(periodFilter);

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
            if (Number(objResult[i].getValue(columns[6])) <= Number(paramperiodo)) {
              for (var col = 0; col < columns.length; col++) {
                if (col == 8) {
                  arrQuiebre[col] = objResult[i].getText(columns[col]);
                } else if (col == 7) {

                  arrQuiebre[col] = objResult[i].getValue(columns[col]) * objResult[i].getValue(columns[10]);

                } else {
                  arrQuiebre[col] = objResult[i].getValue(columns[col]);
                }
              }

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
      var arrQuiebre = new Array();
      // Exedio las unidades
      var DbolStop = false;
      var arrCuatroDigitos = new Array();
      var _contg = 0;
      //var _cont = 0;

      var savedsearch = search.load({
        id: 'customsearch_lmry_co_invent_balanc_trale'
      });

      if (featSubsi) {
        var subsidiaryFilter = search.createFilter({
          name: 'subsidiary',
          operator: search.Operator.IS,
          values: [paramsubsidi]
        });
        savedsearch.filters.push(subsidiaryFilter);
      }

      var periodFields = search.lookupFields({
        type: search.Type.ACCOUNTING_PERIOD,
        id: paramperiodo,
        columns: ['startdate']
      });

      var periodStartDate = periodFields.startdate;

      var periodFilter = search.createFilter({
        name: 'startdate',
        join: 'accountingperiod',
        operator: search.Operator.ONORBEFORE,
        values: [periodStartDate]
      });
      savedsearch.filters.push(periodFilter);

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
            arrQuiebre = new Array();

            for (var col = 0; col < columns.length; col++) {
              if (col == 8) {
                arrQuiebre[col] = objResult[i].getText(columns[col]);
              } else if (col == 7) {
                if (featMulti) {
                  arrQuiebre[col] = objResult[i].getValue(columns[12]);
                } else {
                  arrQuiebre[col] = objResult[i].getValue(columns[7]);
                }
              } else {
                arrQuiebre[col] = objResult[i].getValue(columns[col]);
              }
            }

            arrCuatroDigitos[_contg] = arrQuiebre;
            _contg++;
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
          values: [paramsubsidi]
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
      _NameFile = "COLibroInventarioBalance" + '_' + ip + '_' + companyname + '_' + MM + '_' + YYYY;

      return _NameFile;

    }

    //-------------------------------------------------------------------------------------------------------
    // Graba el archivo en el Gabinete de Archivos
    //-------------------------------------------------------------------------------------------------------
    function SaveFile() {

      var objContext = runtime.getCurrentScript();
      // Ruta de la carpeta contenedora
      var FolderId = objContext.getParameter({
        name: 'custscript_lmry_file_cabinet_rg_co'
      });

      // Almacena en la carpeta de Archivos Generados
      if (FolderId != '' && FolderId != null) {
        // Genera el nombre del archivo
        var fileext;
        var NameFile;

        // Crea el archivo
        fileext = '.xls';
        if (featMulti) {
          if (Number(paramCont) == 0) {
            NameFile = Name_File() + '_' + paramMulti + fileext;
          } else {
            NameFile = Name_File() + '_' + paramMulti + '_' + paramCont + fileext;
          }
        } else {
          if (Number(paramCont) == 0) {
            NameFile = Name_File() + fileext;
          } else {
            NameFile = Name_File() + '_' + paramCont + fileext;
          }
        }

        // Crea el archivo.xls
        var file = fileModulo.create({
          name: NameFile,
          fileType: fileModulo.Type.EXCEL,
          contents: Final_string,
          folder: FolderId
        });

        // Termina de grabar el archivo
        var idfile = file.save();

        // Trae URL de archivo generado
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
          if (Number(paramCont) > 1) {
            var record = recordModulo.create({
              type: 'customrecord_lmry_co_rpt_generator_log',

            });

            //Nombre de Archivo
            record.setValue({
              fieldId: 'custrecord_lmry_co_rg_name',
              value: NameFile
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
            libreria.sendrptuser('CO - Libro de Inventario y Balance 2.0', 3, NameFile);
          } else {
            var record = recordModulo.load({
              type: 'customrecord_lmry_co_rpt_generator_log',
              id: paramidrpt
            });

            //Nombre de Archivo
            record.setValue({
              fieldId: 'custrecord_lmry_co_rg_name',
              value: NameFile
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
            libreria.sendrptuser('CO - Libro de Inventario y Balance 2.0', 3, NameFile);
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
        id: paramidrpt
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
        companyname = ObtainNameSubsidiaria(paramsubsidi);
        companyruc = ObtainFederalIdSubsidiaria(paramsubsidi);

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



    function ValidateCountry(subsidiary) {
      try {
        if (subsidiary != '' && subsidiary != null) {
          var country_obj = search.lookupFields({
            type: search.Type.SUBSIDIARY,
            id: subsidiary,
            columns: ['country']
          });
          if (country_obj.country[0].value == 'MX') {
            return true;
          }
        }
      } catch (err) {
        libreria.sendMail(LMRY_script, ' [ ValidateCountry ] ' + err);
      }
      return false;
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
      var objContext = runtime.getCurrentScript();

      paramsubsidi = objContext.getParameter({
        name: 'custscript_lmry_co_subsi_invbalance'
      });

      paramperiodo = objContext.getParameter({
        name: 'custscript_lmry_co_periodo_invbalance'
      });

      paramMulti = objContext.getParameter({
        name: 'custscript_lmry_co_multibook_invbalance'
      });

      paramidrpt = objContext.getParameter({
        name: 'custscript_lmry_co_idrpt_invbalance'
      });

      paramCont = objContext.getParameter({
        name: 'custscript_lmry_co_cont_invbalance'
      });

      paramBucle = objContext.getParameter({
        name: 'custscript_lmry_co_bucle_invbalance'
      });

      if (paramCont == null) {
        paramCont = 0;
      }

      if (paramBucle == null) {
        paramBucle = 0;
      }

      //Features
      featSubsi = runtime.isFeatureInEffect({
        feature: "SUBSIDIARIES"
      });
      featMulti = runtime.isFeatureInEffect({
        feature: "MULTIBOOK"
      });

      log.error({
        title: 'ENTROfeats',
        details: paramidrpt + ' ' + paramMulti + ' ' + paramsubsidi + ' ' + paramperiodo
      });


      //Period enddate para el nombre del libro
      var period_temp = search.lookupFields({
        type: search.Type.ACCOUNTING_PERIOD,
        id: paramperiodo,
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
      var DD = fecha_format.getDate();

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