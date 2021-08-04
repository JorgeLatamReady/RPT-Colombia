/* = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =\
||   This script for customer center (Time)                     ||
||                                                              ||
||  File Name: LMRY_CO_BalCompTerceros_SCHDL_v2.0.js            ||
||                                                              ||
||  Version Date         Author        Remarks                  ||
||  2.0     Jun 18 2018  LatamReady    Use Script 2.0           ||
\= = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = */
/**
 * @NApiVersion 2.x
 * @NScriptType ScheduledScript
 * @NModuleScope Public
 */
define(["N/record", "N/runtime", "N/file", "N/email", "N/search",
    "N/log", "N/config", "N/task", "N/encode", "N/format", "./CO_Library_Mensual/LMRY_CO_Reportes_LBRY_V2.0.js"
  ],

  function(recordModulo, runtime, fileModulo, email, search, log,
    config, task, encode, format, libreria) {

    var objContext = runtime.getCurrentScript();
    // Nombre del Reporte
    var namereport = 'CO - Balance de Comprobacion por Terceros';
    var LMRY_script = 'LMRY_CO_BalCompTerceros_SCHDL_v2.0.js';

    var paramRecordID = null;
    var paramFileID = null;
    var paramEntityID = null;
    var paramMultibook = null;
    var paramSubsidy = null;
    var paramPeriod = null;
    var paramStep = null;
    var paramLastPuc = null;
    var paramPeriodFin = null;
    var paramAdjustment = null;

    var featureMultibook = false;
    var featureSubdidiary = false;

    var ArrData = new Array();
    var ArrAccounts = new Array();

    var entity_name;
    var multibookName;

    var companyruc;
    var companyname;

    var periodenddate;
    var periodstartdate;
    var periodname;
    var periodnamefinal;

    var flagEmpty = false;

    var entityCustomer = false;
    var entityVendor = false;
    var entityEmployee = false;

    var entity_name;
    var entity_id;
    var entity_nit;

    function execute(context) {
      try {
        ObtenerParametros();
        ObtenerDatosSubsidiaria();

        if (paramStep == 0) {
          ArrAccounts = ObtenerCuentas();

          if (paramFileID != null) {
            var strFile = ObtenerFile();

            ArrData = ConvertToArray(strFile);
          }

          if (ArrData.length != 0) {
            ArrData = CambiarDataCuenta(ArrData);
            paramFileID = saveTemporal(ArrData, 'DATA_CON_CUENTAS_PUC.txt');
            paramStep++;
            LlamarSchedule();

          } else {
            NoData();

            paramLastPuc++;

            if (paramLastPuc < 9) {
              LlamarMapReduce();
            }

            return true;
          }

        } else if (paramStep == 1) {
          if (paramFileID != null) {
            var strFile = ObtenerFile();
            ArrData = ConvertToArray(strFile);
          }

          ArrData.sort(sortFunction);

          function sortFunction(a, b) {
            if (a[0] === b[0]) {
              return 0;
            } else {
              return (a[0] < b[0]) ? -1 : 1;
            }
          }

          paramFileID = saveTemporal(ArrData, 'ORDENADITO_POR_PUCS.txt');
          paramStep++;
          LlamarSchedule();

        } else if (paramStep == 2) {
          if (paramFileID != null) {
            var strFile = ObtenerFile();
            ArrData = ConvertToArray(strFile);
          }

          ArrData = AgruparPorPucsYEntidad(ArrData);
          paramFileID = saveTemporal(ArrData, 'AgrupadoPUCSYENTIDAD.txt');
          paramStep++;
          LlamarSchedule();

        } else if (paramStep == 3) {
          if (paramFileID != null) {
            var strFile = ObtenerFile();
            ArrData = ConvertToArray(strFile);
          }

          ArrData = AgregarArregloCuatroDigitos(ArrData);
          paramFileID = saveTemporal(ArrData, 'TERCEROS_ORDENADOS_PUC.txt');
          paramStep++;
          LlamarSchedule();

        } else if (paramStep == 4) {
          if (paramFileID != null) {
            var strFile = ObtenerFile();
            ArrData = ConvertToArray(strFile);
          }

          if (ArrData.length == 0) {
            flagEmpty = true;
          }

          GeneraArchivo(ArrData);
          paramLastPuc++;

          if (paramLastPuc < 9) {
            //LlamarSchedule();
            LlamarMapReduce();
          }
        }
      } catch (err) {
        log.error('err', err);
        libreria.sendMail(namereport, ' [ execute ] ' + err);
      }
    }

    function LlamarMapReduce() {
      try {
        var params = {};

        params['custscript_lmry_terceros_mprdc_period'] = paramPeriod;

        if (paramPeriodFin != null) {
          params['custscript_lmry_terceros_mprdc_periodFin'] = paramPeriodFin;
        }

        if (featuresubs) {
          params['custscript_lmry_terceros_mprdc_subsi'] = paramSubsidy;
        }

        if (feamultibook) {
          params['custscript_lmry_terceros_mprdc_multi'] = paramMultibook
        }

        if (paramEntityID != null) {
          params['custscript_lmry_terceros_mprdc_entity'] = paramEntityID;
        }

        if (paramRecordID != null) {
          params['custscript_lmry_terceros_mprdc_record'] = paramRecordID;
        }

        params['custscript_lmry_terceros_mprdc_lastpuc'] = paramLastPuc;
        params['custscript_lmry_terceros_mprdc_adjust'] = paramAdjustment;

        var RedirecSchdl = task.create({
          taskType: task.TaskType.MAP_REDUCE,
          scriptId: 'customscript_lmry_co_bcmp_terc_mprd_v2_0',
          deploymentId: 'customdeploy_lmry_co_bcmp_terc_mprd_v2_0',
          params: params
        });

        RedirecSchdl.submit();
      } catch (err) {
        log.error('err', err);
      }
    }

    function AgruparPorPucsYEntidad(ArrTemp) {
      var ArrReturn = new Array();

      ArrReturn.push(ArrTemp[0]);

      for (var i = 1; i < ArrTemp.length; i++) {
        if (ArrTemp[i][0].trim() != '') {
          var intLength = ArrReturn.length;
          for (var j = 0; j < intLength; j++) {
            if (ArrTemp[i][0] == ArrReturn[j][0] && ArrTemp[i][14].trim() == ArrReturn[j][14].trim()) {
              ArrReturn[j][2] = Math.abs(ArrReturn[j][2]) + Math.abs(ArrTemp[i][2]);
              ArrReturn[j][3] = Math.abs(ArrReturn[j][3]) + Math.abs(ArrTemp[i][3]);
              ArrReturn[j][4] = Math.abs(ArrReturn[j][4]) + Math.abs(ArrTemp[i][4]);
              ArrReturn[j][5] = Math.abs(ArrReturn[j][5]) + Math.abs(ArrTemp[i][5]);
              ArrReturn[j][6] = Math.abs(ArrReturn[j][6]) + Math.abs(ArrTemp[i][6]);
              ArrReturn[j][7] = Math.abs(ArrReturn[j][7]) + Math.abs(ArrTemp[i][7]);
              break;
            }
            if (j == ArrReturn.length - 1) {
              ArrReturn.push(ArrTemp[i]);
            }
          }
        }
      }

      return ArrReturn;
    }

    function ConvertirAString(ArrTemp) {
      var str_return = '';

      for (var i = 0; i < ArrTemp.length; i++) {
        for (var j = 0; j < ArrTemp[i].length; j++) {
          str_return += ArrTemp[i][j];
          str_return += '|';
        }
        str_return += '\r\n';
      }

      return str_return;
    }

    function LlamarSchedule() {
      var params = {};

      params['custscript_lmry_co_terce_schdl_period'] = paramPeriod;

      params['custscript_lmry_co_terce_schdl_fileid'] = paramFileID;

      if (featuresubs) {
        params['custscript_lmry_co_terce_schdl_subsi'] = paramSubsidy;
      }

      if (feamultibook) {
        params['custscript_lmry_co_terce_schdl_multi'] = paramMultibook
      }

      if (paramEntityID != null) {
        params['custscript_lmry_co_terce_schdl_entity'] = paramEntityID;
      }

      params['custscript_lmry_co_terce_schdl_step'] = Number(paramStep);

      params['custscript_lmry_co_terce_schdl_lastpuc'] = paramLastPuc;

      if (paramRecordID != null) {
        params['custscript_lmry_co_terce_schdl_recordid'] = paramRecordID;
      }

      if (paramPeriodFin != null) {
        params['custscript_lmry_co_terce_schdl_periodfin'] = paramPeriodFin;
      }

      var RedirecSchdl = task.create({
        taskType: task.TaskType.SCHEDULED_SCRIPT,
        scriptId: 'customscript_lmry_co_bcmp_ter_schdl_v2_0',
        deploymentId: 'customdeploy_lmry_co_bcmp_ter_schdl_v2_0',
        params: params
      });
      RedirecSchdl.submit();
    }

    function saveTemporal(arrTemp, Final_NameFile) {
      var strTemp = '';

      for (var i = 0; i < arrTemp.length; i++) {
        for (var j = 0; j < arrTemp[i].length; j++) {
          strTemp += arrTemp[i][j];
          strTemp += '|';
        }
        strTemp += '\r\n';
      }

      var FolderId = objContext.getParameter({
        name: 'custscript_lmry_file_cabinet_rg_co'
      });

      // Almacena en la carpeta de Archivos Generados
      if (FolderId != '' && FolderId != null) {
        // Crea el archivo.xls
        var file = fileModulo.create({
          name: Final_NameFile,
          fileType: fileModulo.Type.PLAINTEXT,
          contents: strTemp,
          folder: FolderId
        });

        var idfile = file.save();
      }

      return idfile;
    }

    function ObtenerDatosSubsidiaria() {
      var configpage = config.load({
        type: config.Type.COMPANY_INFORMATION
      });

      if (featuresubs) {
        companyname = ObtainNameSubsidiaria(paramSubsidy);
        companyruc = ObtainFederalIdSubsidiaria(paramSubsidy);
      } else {
        companyruc = configpage.getValue('employerid');
        companyname = configpage.getValue('legalname');
      }

      companyruc = companyruc.replace(' ', '');
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
        //libreria.sendMail(LMRY_script, ' [ ObtainNameSubsidiaria ] ' + err);
      }
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
        //libreria.sendMail(LMRY_script, ' [ ObtainFederalIdSubsidiaria ] ' + err);
      }
    }

    function GeneraArchivo(ArrSaldoFinal) {
      var DbolStop = false;

      var flagAllZero = true;

      if (ArrSaldoFinal.length != null && ArrSaldoFinal.length != 0 && !flagEmpty) {

        var xlsArchivo = '';
        xlsArchivo = '<?xml version="1.0" encoding="UTF-8" ?><?mso-application progid="Excel.Sheet"?>';
        xlsArchivo += '<Workbook xmlns="urn:schemas-microsoft-com:office:spreadsheet" ';
        xlsArchivo += 'xmlns:o="urn:schemas-microsoft-com:office:office" ';
        xlsArchivo += 'xmlns:x="urn:schemas-microsoft-com:office:excel" ';
        xlsArchivo += 'xmlns:ss="urn:schemas-microsoft-com:office:spreadsheet" ';
        xlsArchivo += 'xmlns:html="http://www.w3.org/TR/REC-html40">';
        xlsArchivo += '<Styles>';
        xlsArchivo += '<Style ss:ID="s21"><Font ss:Bold="1" ss:Size="12" /><Alignment ss:Horizontal="Center" ss:Vertical="Bottom"/></Style>';
        xlsArchivo += '<Style ss:ID="s22"><Font ss:Bold="1"/><Alignment ss:Vertical="Bottom"/></Style>';
        xlsArchivo += '<Style ss:ID="s23"><Font ss:Bold="1"/><Alignment ss:Vertical="Bottom"/><NumberFormat ss:Format="_(* #,##0.00_);_(* \(#,##0.00\);_(* &quot;-&quot;??_);_(@_)"/></Style>';
        xlsArchivo += '<Style ss:ID="s24"><NumberFormat ss:Format="_(* #,##0.00_);_(* \(#,##0.00\);_(* &quot;-&quot;??_);_(@_)"/></Style>';
        xlsArchivo += '</Styles><Worksheet ss:Name="Sheet1">';


        xlsArchivo += '<Table>';
        xlsArchivo += '<Column ss:AutoFitWidth="0" ss:Width="100"/>';
        xlsArchivo += '<Column ss:AutoFitWidth="0" ss:Width="150"/>';
        xlsArchivo += '<Column ss:AutoFitWidth="0" ss:Width="100"/>';
        xlsArchivo += '<Column ss:AutoFitWidth="0" ss:Width="100"/>';
        xlsArchivo += '<Column ss:AutoFitWidth="0" ss:Width="100"/>';
        xlsArchivo += '<Column ss:AutoFitWidth="0" ss:Width="100"/>';
        xlsArchivo += '<Column ss:AutoFitWidth="0" ss:Width="100"/>';
        xlsArchivo += '<Column ss:AutoFitWidth="0" ss:Width="100"/>';

        //Cabecera
        var xlsCabecera = '';
        xlsCabecera = '<Row>';
        xlsCabecera += '<Cell></Cell>';
        xlsCabecera += '<Cell></Cell>';
        xlsCabecera += '<Cell></Cell>';
        xlsCabecera += '<Cell ss:StyleID="s21"><Data ss:Type="String"> LIBRO DE BALANCE DE COMPROBACION POR TERCEROS </Data></Cell>';
        xlsCabecera += '</Row>';
        xlsCabecera += '<Row></Row>';
        xlsCabecera += '<Row>';
        xlsCabecera += '<Cell></Cell>';
        xlsCabecera += '<Cell></Cell>';
        xlsCabecera += '<Cell  ss:StyleID="s22"><Data ss:Type="String">Razon Social: ' + companyname + '</Data></Cell>';
        xlsCabecera += '</Row>';
        xlsCabecera += '<Row>';
        xlsCabecera += '<Cell></Cell>';
        xlsCabecera += '<Cell></Cell>';
        xlsCabecera += '<Cell  ss:StyleID="s22"><Data ss:Type="String">NIT: ' + companyruc + '</Data></Cell>';
        xlsCabecera += '</Row>';
        xlsCabecera += '<Row>';
        xlsCabecera += '<Cell></Cell>';
        xlsCabecera += '<Cell></Cell>';
        if (paramPeriodFin != null && paramPeriodFin != '') {
          xlsCabecera += '<Cell  ss:StyleID="s22"><Data ss:Type="String">Periodo: ' + periodname + ' - ' + periodnamefinal + '</Data></Cell>';
        } else {
          xlsCabecera += '<Cell  ss:StyleID="s22"><Data ss:Type="String">Periodo: ' + periodstartdate + ' - ' + periodenddate + '</Data></Cell>';
        }
        xlsCabecera += '</Row>';
        if ((feamultibook || feamultibook == 'T') && (paramMultibook != '' && paramMultibook != null)) {
          xlsCabecera += '<Row>';
          xlsCabecera += '<Cell></Cell>';
          xlsCabecera += '<Cell></Cell>';
          xlsCabecera += '<Cell ss:StyleID="s22"><Data ss:Type="String">Multibooking: ' + multibookName + '</Data></Cell>';
          xlsCabecera += '</Row>';
        }

        if (paramEntityID != null && paramEntityID != '') {
          var flag_entity = ObtenerEntidad(paramEntityID);
          var name_enti = ValidarAcentos(entity_name);
          if (flag_entity) {
            xlsCabecera += '<Row>';
            xlsCabecera += '<Cell></Cell>';
            xlsCabecera += '<Cell></Cell>';
            xlsCabecera += '<Cell ss:StyleID="s22"><Data ss:Type="String">Entidad: ' + name_enti + '</Data></Cell>';
            xlsCabecera += '</Row>';
          }
        }

        xlsCabecera += '<Row></Row>';
        xlsCabecera += '<Row></Row>';
        xlsCabecera += '<Row>' +
          '<Cell></Cell>' +
          '<Cell></Cell>' +
          '<Cell></Cell>' +
          '<Cell></Cell>' +
          '<Cell ss:StyleID="s21"><Data ss:Type="String"> Saldo Anterior </Data></Cell>' +
          '<Cell></Cell>' +
          '<Cell ss:StyleID="s21"><Data ss:Type="String"> Movimiento </Data></Cell>' +
          '<Cell></Cell>' +
          '<Cell ss:StyleID="s21"><Data ss:Type="String"> Nuevo Saldo </Data></Cell>' +
          '</Row>';

        xlsCabecera += '<Row>' +
          '<Cell ss:StyleID="s21"><Data ss:Type="String"> Cuenta </Data></Cell>' +
          '<Cell ss:StyleID="s21"><Data ss:Type="String"> Denominacion </Data></Cell>' +
          '<Cell ss:StyleID="s21"><Data ss:Type="String"> Entidad </Data></Cell>' +
          '<Cell ss:StyleID="s21"><Data ss:Type="String"> NIT </Data></Cell>' +
          '<Cell ss:StyleID="s21"><Data ss:Type="String"> Debe </Data></Cell>' +
          '<Cell ss:StyleID="s21"><Data ss:Type="String"> Haber </Data></Cell>' +
          '<Cell ss:StyleID="s21"><Data ss:Type="String"> Debe </Data></Cell>' +
          '<Cell ss:StyleID="s21"><Data ss:Type="String"> Haber </Data></Cell>' +
          '<Cell ss:StyleID="s21"><Data ss:Type="String"> Debe </Data></Cell>' +
          '<Cell ss:StyleID="s21"><Data ss:Type="String"> Haber </Data></Cell>' +
          '</Row>';

        var xlsString = xlsArchivo + xlsCabecera;

        var _TotalSIDebe = 0.0;
        var _TotalSIHaber = 0.0;
        var _TotalMovDebe = 0.0;
        var _TotalMovHaber = 0.0;
        var _TotalSFDebe = 0.0;
        var _TotalSFHaber = 0.0;

        // Formato de la celda
        _StyleTxt = ' ss:StyleID="s22" ';
        _StyleNum = ' ss:StyleID="s23" ';

        for (var i = 0; i < ArrSaldoFinal.length; i++) {
          if (Math.abs(ArrSaldoFinal[i][2]) == 0 && Math.abs(ArrSaldoFinal[i][3]) == 0 &&
            Math.abs(ArrSaldoFinal[i][4]) == 0 && Math.abs(ArrSaldoFinal[i][5]) == 0 &&
            Math.abs(ArrSaldoFinal[i][6]) == 0 && Math.abs(ArrSaldoFinal[i][7]) == 0) {

          } else {
            if (ArrSaldoFinal[i][0].charAt(0) == paramLastPuc) {

              flagAllZero = false;

              xlsString += '<Row>';

              //////////////////////////////////
              //numero de cuenta
              if (ArrSaldoFinal[i][0] != '' && ArrSaldoFinal[i][0] != null && ArrSaldoFinal[i][0] != '- None -') {

                xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String">' + ArrSaldoFinal[i][0] + '</Data></Cell>';
              } else {
                xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String"></Data></Cell>';
              }

              //Denominacion
              if (ArrSaldoFinal[i][1].length > 0 && ArrSaldoFinal[i][0] != '- None -') {
                var s = ValidarAcentos(ArrSaldoFinal[i][1]);
                xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String">' + s + '</Data></Cell>';
              } else {
                xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String"></Data></Cell>';
              }
              //Entidad
              if (ArrSaldoFinal[i][14] != null && ArrSaldoFinal[i][14] != '' && ArrSaldoFinal[i][14] != '- None -') {
                var json_entity = JSON.parse(ArrSaldoFinal[i][14]);

                var nombre = '';

                if (json_entity != null) {
                  nombre = ValidarAcentos(json_entity.name);
                }

                xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String">' + nombre + '</Data></Cell>';
              } else {
                xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String"></Data></Cell>';
              }

              //Entidad
              if (ArrSaldoFinal[i][14] != null && ArrSaldoFinal[i][14] != '' && ArrSaldoFinal[i][14] != '- None -') {
                var json_entity = JSON.parse(ArrSaldoFinal[i][14]);

                var nit = '';

                if (json_entity != null) {
                  var temp_nit = json_entity.nit;
                  nit = ((temp_nit).replace(/-/g, '')).replace(/_/g, '');
                }

                xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String">' + nit + '</Data></Cell>';
              } else {
                xlsString += '<Cell ss:StyleID="s22"><Data ss:Type="String"></Data></Cell>';
              }

              /////////////////////////////////
              if (ArrSaldoFinal[i][0].length == 1 || ArrSaldoFinal[i][0].length == 2 || ArrSaldoFinal[i][0].length == 4) {
                var saldo_antes = Math.abs(ArrSaldoFinal[i][2]) - Math.abs(ArrSaldoFinal[i][3]);
                var saldo_actual = Math.abs(ArrSaldoFinal[i][2]) + Math.abs(ArrSaldoFinal[i][4]) - Math.abs(ArrSaldoFinal[i][3]) - Math.abs(ArrSaldoFinal[i][5]);
                var movimientos = Math.abs(ArrSaldoFinal[i][4]) - Math.abs(ArrSaldoFinal[i][5]);

                if (saldo_antes < 0) {
                  //Haber Antes
                  xlsString += '<Cell ' + _StyleNum + '><Data ss:Type="Number">' + 0.0 + '</Data></Cell>';
                  //Debe Antes
                  xlsString += '<Cell ' + _StyleNum + '><Data ss:Type="Number">' + Math.abs(saldo_antes) + '</Data></Cell>';
                } else {
                  if (saldo_antes > 0) {
                    //Haber Antes
                    xlsString += '<Cell ' + _StyleNum + '><Data ss:Type="Number">' + Math.abs(saldo_antes) + '</Data></Cell>';
                    //Debe Antes
                    xlsString += '<Cell ' + _StyleNum + '><Data ss:Type="Number">' + 0.0 + '</Data></Cell>';
                  } else {
                    xlsString += '<Cell ' + _StyleNum + '><Data ss:Type="Number">' + 0.0 + '</Data></Cell>';
                    xlsString += '<Cell ' + _StyleNum + '><Data ss:Type="Number">' + 0.0 + '</Data></Cell>';

                  }
                }

                if (movimientos < 0) {
                  //Haber Antes
                  xlsString += '<Cell ' + _StyleNum + '><Data ss:Type="Number">' + 0.0 + '</Data></Cell>';
                  //Debe Antes
                  xlsString += '<Cell ' + _StyleNum + '><Data ss:Type="Number">' + Math.abs(movimientos) + '</Data></Cell>';
                } else {
                  if (movimientos > 0) {
                    //Haber Antes
                    xlsString += '<Cell ' + _StyleNum + '><Data ss:Type="Number">' + Math.abs(movimientos) + '</Data></Cell>';
                    //Debe Antes
                    xlsString += '<Cell ' + _StyleNum + '><Data ss:Type="Number">' + 0.0 + '</Data></Cell>';
                  } else {
                    xlsString += '<Cell ' + _StyleNum + '><Data ss:Type="Number">' + 0.0 + '</Data></Cell>';
                    xlsString += '<Cell ' + _StyleNum + '><Data ss:Type="Number">' + 0.0 + '</Data></Cell>';

                  }
                }

                if (saldo_actual < 0) {
                  //Haber Saldo
                  xlsString += '<Cell ' + _StyleNum + '><Data ss:Type="Number">' + 0.0 + '</Data></Cell>';

                  //Debe Saldo
                  xlsString += '<Cell ' + _StyleNum + '><Data ss:Type="Number">' + Math.abs(saldo_actual) + '</Data></Cell>';
                } else if (saldo_actual > 0) {
                  //Haber Saldo
                  xlsString += '<Cell ' + _StyleNum + '><Data ss:Type="Number">' + Math.abs(saldo_actual) + '</Data></Cell>';

                  //Debe Saldo
                  xlsString += '<Cell ' + _StyleNum + '><Data ss:Type="Number">' + 0.0 + '</Data></Cell>';

                }

                if (ArrSaldoFinal[i][0].length == 1) {
                  var a = Math.abs(ArrSaldoFinal[i][2]) - Math.abs(ArrSaldoFinal[i][3]);
                  var b = Math.abs(ArrSaldoFinal[i][4]) - Math.abs(ArrSaldoFinal[i][5]);
                  var c = Math.abs(ArrSaldoFinal[i][2]) - Math.abs(ArrSaldoFinal[i][3]) + Math.abs(ArrSaldoFinal[i][4]) - Math.abs(ArrSaldoFinal[i][5]);

                  if (a >= 0) {
                    _TotalSIDebe += a;
                  } else {
                    _TotalSIHaber += a;
                  }

                  if (b >= 0) {
                    _TotalMovDebe += b;
                  } else {
                    _TotalMovHaber += b;
                  }

                  if (c >= 0) {
                    _TotalSFDebe += c;
                  } else {
                    _TotalSFHaber += c;
                  }
                }

              } else {
                //Debe Antes
                xlsString += '<Cell ' + _StyleNum + '><Data ss:Type="Number">' + Math.abs(Number(ArrSaldoFinal[i][2])) + '</Data></Cell>';
                //Haber Antes
                xlsString += '<Cell ' + _StyleNum + '><Data ss:Type="Number">' + Math.abs(Number(ArrSaldoFinal[i][3])) + '</Data></Cell>';
                //Haber Movimientos
                xlsString += '<Cell ' + _StyleNum + '><Data ss:Type="Number">' + Math.abs(Number(ArrSaldoFinal[i][4])) + '</Data></Cell>';
                //Debe Movimientos
                xlsString += '<Cell ' + _StyleNum + '><Data ss:Type="Number">' + Math.abs(Number(ArrSaldoFinal[i][5])) + '</Data></Cell>';
                //Debe Saldo
                xlsString += '<Cell ' + _StyleNum + '><Data ss:Type="Number">' + Math.abs(Number(ArrSaldoFinal[i][6])) + '</Data></Cell>';
                //Haber Saldo
                xlsString += '<Cell ' + _StyleNum + '><Data ss:Type="Number">' + Math.abs(Number(ArrSaldoFinal[i][7])) + '</Data></Cell>';

              }
              xlsString += '</Row>';
            }
          }
        }

        xlsString += '<Row>';
        xlsString += '<Cell></Cell>';
        xlsString += '<Cell></Cell>';
        xlsString += '<Cell ' + _StyleTxt + '><Data ss:Type="String">TOTALES</Data></Cell>';
        xlsString += '<Cell></Cell>';
        xlsString += '<Cell ' + _StyleNum + '><Data ss:Type="Number">' + Math.abs(_TotalSIDebe) + '</Data></Cell>';
        xlsString += '<Cell ' + _StyleNum + '><Data ss:Type="Number">' + Math.abs(_TotalSIHaber) + '</Data></Cell>';
        xlsString += '<Cell ' + _StyleNum + '><Data ss:Type="Number">' + Math.abs(_TotalMovDebe) + '</Data></Cell>';
        xlsString += '<Cell ' + _StyleNum + '><Data ss:Type="Number">' + Math.abs(_TotalMovHaber) + '</Data></Cell>';
        xlsString += '<Cell ' + _StyleNum + '><Data ss:Type="Number">' + Math.abs(_TotalSFDebe) + '</Data></Cell>';
        xlsString += '<Cell ' + _StyleNum + '><Data ss:Type="Number">' + Math.abs(_TotalSFHaber) + '</Data></Cell>';
        xlsString += '</Row>';

        xlsString += '</Table></Worksheet></Workbook>';

        // Se arma el archivo EXCEL
        Final_string = encode.convert({
          string: xlsString,
          inputEncoding: encode.Encoding.UTF_8,
          outputEncoding: encode.Encoding.BASE_64
        });

        if (flagAllZero) {
          NoData();
        } else {
          savefile(Final_string);
        }

        paramRecordID = null;
      } else {
        NoData();
      }
    }

    function NoData() {
      var usuarioTemp = runtime.getCurrentUser();
      var usuario = usuarioTemp.name;

      if (paramRecordID != null && paramRecordID != '') {
        var record = recordModulo.load({
          type: 'customrecord_lmry_co_rpt_generator_log',
          id: paramRecordID
        });

        paramRecordID = null;
      } else {
        var record = recordModulo.create({
          type: 'customrecord_lmry_co_rpt_generator_log'
        });
      }

      //Nombre de Archivo
      record.setValue({
        fieldId: 'custrecord_lmry_co_rg_name',
        value: 'No existe informacion para los criterios seleccionados'
      });

      //Nombre de Reporte
      record.setValue({
        fieldId: 'custrecord_lmry_co_rg_transaction',
        value: namereport
      });

      //Nombre de Subsidiaria
      record.setValue({
        fieldId: 'custrecord_lmry_co_rg_subsidiary',
        value: companyname
      });

      //Periodo
      if (periodnamefinal != null && periodnamefinal != '') {
        record.setValue({
          fieldId: 'custrecord_lmry_co_rg_postingperiod',
          value: periodname + ' - ' + periodnamefinal
        });
      } else {
        record.setValue({
          fieldId: 'custrecord_lmry_co_rg_postingperiod',
          value: periodname
        });
      }

      //Multibook
      if (feamultibook || feamultibook == 'T') {
        record.setValue({
          fieldId: 'custrecord_lmry_co_rg_multibook',
          value: multibookName
        });
      }

      if (paramEntityID != null) {
        record.setValue({
          fieldId: 'custrecord_lmry_co_rg_entity',
          value: entity_name
        });
      }

      //Creado Por
      record.setValue({
        fieldId: 'custrecord_lmry_co_rg_employee',
        value: usuario
      });

      record.save();
    }

    function AgregarArregloCuatroDigitos(ArrSaldoFinal) {
      for (var y = 0; y < ArrSaldoFinal.length; y++) {
        if (ArrSaldoFinal[y][1] == '' || ArrSaldoFinal[y][1] == null) {
          ArrSaldoFinal[y][1] = 0.0;
        }

        if (ArrSaldoFinal[y][2] == '' || ArrSaldoFinal[y][2] == null) {
          ArrSaldoFinal[y][2] = 0.0;
        }

        if (ArrSaldoFinal[y][3] == '' || ArrSaldoFinal[y][3] == null) {
          ArrSaldoFinal[y][3] = 0.0;
        }

        if (ArrSaldoFinal[y][4] == '' || ArrSaldoFinal[y][4] == null) {
          ArrSaldoFinal[y][4] = 0.0;
        }

        if (ArrSaldoFinal[y][5] == '' || ArrSaldoFinal[y][5] == null) {
          ArrSaldoFinal[y][5] = 0.0;
        }

        if (ArrSaldoFinal[y][6] == '' || ArrSaldoFinal[y][6] == null) {
          ArrSaldoFinal[y][6] = 0.0;
        }
      }
      /*
       * ARRAY FINAL
       * 0.  cuenta 6
       * 1.  denominacion 6
       * 2.  debitos antes
       * 3.  creditos antes
       * 4.  debitos actual
       * 5.  creditos actual
       * 6.  nuevo saldo debitos
       * 7.  nuevo saldo creditos
       * 8.  cuenta 4 digitos
       * 9.  denominacion 4 digitos
       * 10. cuenta 2 digitos
       * 11. denominacion 2 digitos
       * 12. cuenta 1 digito
       * 13. denominacion 1 digito
       * 14. entity
       */

      var cuenta_aux = ArrSaldoFinal[0][8];

      var array_4_digitos = new Array();

      array_4_digitos[0] = cuenta_aux;
      array_4_digitos[1] = ArrSaldoFinal[0][9];
      array_4_digitos[2] = 0.0;
      array_4_digitos[3] = 0.0;
      array_4_digitos[4] = 0.0;
      array_4_digitos[5] = 0.0;
      array_4_digitos[6] = 0.0;
      array_4_digitos[7] = 0.0;
      array_4_digitos[8] = ArrSaldoFinal[0][8];
      array_4_digitos[9] = ArrSaldoFinal[0][9];
      array_4_digitos[10] = ArrSaldoFinal[0][10];
      array_4_digitos[11] = ArrSaldoFinal[0][11];
      array_4_digitos[12] = ArrSaldoFinal[0][12];
      array_4_digitos[13] = ArrSaldoFinal[0][13];
      array_4_digitos[14] = '';

      //Agregar al  del array
      ArrSaldoFinal.splice(0, 0, array_4_digitos);

      var array_cuentas = new Array();

      array_cuentas[0] = array_4_digitos;

      var cont = 1;
      for (var i = 0; i < ArrSaldoFinal.length; i++) {
        if (ArrSaldoFinal[i][8] != cuenta_aux) {
          cuenta_aux = ArrSaldoFinal[i][8];
          var array_aux = new Array();

          array_aux[0] = cuenta_aux;
          array_aux[1] = ArrSaldoFinal[i][9];
          array_aux[2] = 0.0;
          array_aux[3] = 0.0;
          array_aux[4] = 0.0;
          array_aux[5] = 0.0;
          array_aux[6] = 0.0;
          array_aux[7] = 0.0;
          array_aux[8] = ArrSaldoFinal[i][8];
          array_aux[9] = ArrSaldoFinal[i][9];
          array_aux[10] = ArrSaldoFinal[i][10];
          array_aux[11] = ArrSaldoFinal[i][11];
          array_aux[12] = ArrSaldoFinal[i][12];
          array_aux[13] = ArrSaldoFinal[i][13];
          array_aux[14] = '';

          array_cuentas[cont] = array_aux;
          cont++;
          ArrSaldoFinal.splice(i, 0, array_aux);
        }
      }

      //calcular montos de cuentas
      for (var i = 0; i < array_cuentas.length; i++) {
        for (var j = 0; j < ArrSaldoFinal.length; j++) {
          if (array_cuentas[i][0] == ArrSaldoFinal[j][8]) {
            array_cuentas[i][2] += Number(ArrSaldoFinal[j][2]);
            array_cuentas[i][3] += Number(ArrSaldoFinal[j][3]);
            array_cuentas[i][4] += Number(ArrSaldoFinal[j][4]);
            array_cuentas[i][5] += Number(ArrSaldoFinal[j][5]);
            array_cuentas[i][6] += Number(ArrSaldoFinal[j][6]);
            array_cuentas[i][7] += Number(ArrSaldoFinal[j][7]);
          }
        }
      }

      //reemplazar array vacio en el ArrSaldoFinal
      for (var i = 0; i < array_cuentas.length; i++) {
        for (var j = 0; j < ArrSaldoFinal.length; j++) {
          if (array_cuentas[i][0] == ArrSaldoFinal[j][0]) {
            ArrSaldoFinal[j] = array_cuentas[i];
          }
        }
      }

      ArrSaldoFinal = AgregarArregloDosDigitos(ArrSaldoFinal);

      return ArrSaldoFinal;
    }

    function AgregarArregloDosDigitos(ArrSaldoFinal) {
      /*
       * ARRAY FINAL
       * 0.  cuenta 6
       * 1.  denominacion 6
       * 2.  debitos antes
       * 3.  creditos antes
       * 4.  debitos actual
       * 5.  creditos actual
       * 6.  nuevo saldo debitos
       * 7.  nuevo saldo creditos
       * 8.  cuenta 4 digitos
       * 9.  denominacion 4 digitos
       * 10. cuenta 2 digitos
       * 11. denominacion 2 digitos
       * 12. cuenta 1 digito
       * 13. denominacion 1 digito
       * 14. entity
       */

      var grupo_aux = ArrSaldoFinal[0][10];

      var array_aux_uno = new Array();

      array_aux_uno[0] = grupo_aux;
      array_aux_uno[1] = ArrSaldoFinal[0][11];
      array_aux_uno[2] = 0;
      array_aux_uno[3] = 0;
      array_aux_uno[4] = 0;
      array_aux_uno[5] = 0;
      array_aux_uno[6] = 0;
      array_aux_uno[7] = 0;
      array_aux_uno[8] = ArrSaldoFinal[0][8];
      array_aux_uno[9] = ArrSaldoFinal[0][9];
      array_aux_uno[10] = ArrSaldoFinal[0][10];
      array_aux_uno[11] = ArrSaldoFinal[0][11];
      array_aux_uno[12] = ArrSaldoFinal[0][12];
      array_aux_uno[13] = ArrSaldoFinal[0][13];
      array_aux_uno[14] = '';

      ArrSaldoFinal.splice(0, 0, array_aux_uno);

      var array_cuentas = new Array();

      array_cuentas[0] = array_aux_uno;

      var cont = 1;

      //quiebre de grupo
      for (var i = 0; i < ArrSaldoFinal.length; i++) {
        if (grupo_aux != ArrSaldoFinal[i][10]) {
          grupo_aux = ArrSaldoFinal[i][10];
          var array_aux = new Array();
          /* 0 -> cuenta
           * 1 -> denominacion
           * 2 -> debe antes
           * 3 -> haber antes
           * 4 -> debe actual
           * 5 -> haber actual
           * 6 -> debe nuevo saldo
           * 7 -> haber nuevo saldo
           */
          array_aux[0] = grupo_aux;
          array_aux[1] = ArrSaldoFinal[i][11];
          array_aux[2] = 0.0;
          array_aux[3] = 0.0;
          array_aux[4] = 0.0;
          array_aux[5] = 0.0;
          array_aux[6] = 0.0;
          array_aux[7] = 0.0;
          array_aux[8] = ArrSaldoFinal[i][8];
          array_aux[9] = ArrSaldoFinal[i][9];
          array_aux[10] = ArrSaldoFinal[i][10];
          array_aux[11] = ArrSaldoFinal[i][11];
          array_aux[12] = ArrSaldoFinal[i][12];
          array_aux[13] = ArrSaldoFinal[i][13];
          array_aux[14] = '';

          array_cuentas[cont] = array_aux;
          cont++;
          ArrSaldoFinal.splice(i, 0, array_aux);
        }
      }

      //calcular montos de cuentas
      for (var i = 0; i < array_cuentas.length; i++) {
        for (var j = 0; j < ArrSaldoFinal.length; j++) {
          if (array_cuentas[i][0] == ArrSaldoFinal[j][10] && ArrSaldoFinal[j][0].length == 6) {
            array_cuentas[i][2] += Number(ArrSaldoFinal[j][2]);
            array_cuentas[i][3] += Number(ArrSaldoFinal[j][3]);
            array_cuentas[i][4] += Number(ArrSaldoFinal[j][4]);
            array_cuentas[i][5] += Number(ArrSaldoFinal[j][5]);
            array_cuentas[i][6] += Number(ArrSaldoFinal[j][6]);
            array_cuentas[i][7] += Number(ArrSaldoFinal[j][7]);
          }
        }
      }

      //reemplazar array vacio del ArrSaldoFinal
      for (var i = 0; i < array_cuentas.length; i++) {
        for (var j = 0; j < ArrSaldoFinal.length; j++) {
          if (array_cuentas[i][0] == ArrSaldoFinal[j][0]) {
            ArrSaldoFinal[j] = array_cuentas[i];
          }
        }
      }

      ArrSaldoFinal = AgregarArregloUnDigito(ArrSaldoFinal);

      return ArrSaldoFinal;
    }

    function AgregarArregloUnDigito(ArrSaldoFinal) {
      /*
       * ARRAY FINAL
       * 0.  cuenta 6
       * 1.  denominacion 6
       * 2.  debitos antes
       * 3.  creditos antes
       * 4.  debitos actual
       * 5.  creditos actual
       * 6.  nuevo saldo debitos
       * 7.  nuevo saldo creditos
       * 8.  cuenta 4 digitos
       * 9.  denominacion 4 digitos
       * 10. cuenta 2 digitos
       * 11. denominacion 2 digitos
       * 12. cuenta 1 digito
       * 13. denominacion 1 digito
       * 14. entity
       */
      var clase_aux = ArrSaldoFinal[0][12];

      var array_aux_uno = new Array();
      array_aux_uno[0] = clase_aux;
      array_aux_uno[1] = ArrSaldoFinal[0][13];
      array_aux_uno[2] = 0.0;
      array_aux_uno[3] = 0.0;
      array_aux_uno[4] = 0.0;
      array_aux_uno[5] = 0.0;
      array_aux_uno[6] = 0.0;
      array_aux_uno[7] = 0.0;
      array_aux_uno[8] = ArrSaldoFinal[0][8];
      array_aux_uno[9] = ArrSaldoFinal[0][9];
      array_aux_uno[10] = ArrSaldoFinal[0][10];
      array_aux_uno[11] = ArrSaldoFinal[0][11];
      array_aux_uno[12] = ArrSaldoFinal[0][12];
      array_aux_uno[13] = ArrSaldoFinal[0][13];
      array_aux_uno[14] = '';

      ArrSaldoFinal.splice(0, 0, array_aux_uno);

      var array_cuentas = new Array();

      array_cuentas[0] = array_aux_uno;

      var cont = 1;

      //quiebre de grupo
      for (var i = 0; i < ArrSaldoFinal.length; i++) {
        if (ArrSaldoFinal[i][12] != clase_aux) {
          clase_aux = ArrSaldoFinal[i][12];
          var array_aux = new Array();
          /* 0 -> cuenta
           * 1 -> denominacion
           * 2 -> debe antes
           * 3 -> haber antes
           * 4 -> debe actual
           * 5 -> haber actual
           * 6 -> debe nuevo saldo
           * 7 -> haber nuevo saldo
           */
          array_aux[0] = clase_aux;
          array_aux[1] = ArrSaldoFinal[i][13];
          array_aux[2] = 0.0;
          array_aux[3] = 0.0;
          array_aux[4] = 0.0;
          array_aux[5] = 0.0;
          array_aux[6] = 0.0;
          array_aux[7] = 0.0;
          array_aux[8] = ArrSaldoFinal[i][8];
          array_aux[9] = ArrSaldoFinal[i][9];
          array_aux[10] = ArrSaldoFinal[i][10];
          array_aux[11] = ArrSaldoFinal[i][11];
          array_aux[12] = ArrSaldoFinal[i][12];
          array_aux[13] = ArrSaldoFinal[i][13];
          array_aux[14] = '';

          array_cuentas[cont] = array_aux;
          cont++;
          ArrSaldoFinal.splice(i, 0, array_aux);
        }
      }
      //calcular montos de cuentas
      for (var i = 0; i < array_cuentas.length; i++) {

        for (var j = 0; j < ArrSaldoFinal.length; j++) {
          if (array_cuentas[i][0] == ArrSaldoFinal[j][12] && ArrSaldoFinal[j][0] != null && ArrSaldoFinal[j][0].length == 6) {
            array_cuentas[i][2] += Number(ArrSaldoFinal[j][2]);
            array_cuentas[i][3] += Number(ArrSaldoFinal[j][3]);
            array_cuentas[i][4] += Number(ArrSaldoFinal[j][4]);
            array_cuentas[i][5] += Number(ArrSaldoFinal[j][5]);
            array_cuentas[i][6] += Number(ArrSaldoFinal[j][6]);
            array_cuentas[i][7] += Number(ArrSaldoFinal[j][7]);
          }
        }
      }

      //reemplazar array vacio del ArrSaldoFinal
      for (var i = 0; i < array_cuentas.length; i++) {
        for (var j = 0; j < ArrSaldoFinal.length; j++) {
          if (array_cuentas[i][0] == ArrSaldoFinal[j][0]) {
            ArrSaldoFinal[j] = array_cuentas[i];
          }
        }
      }

      return ArrSaldoFinal;
    }

    function OrdenarPorPucs(array) {
      /*if (origArray.length <= 1) {
          return origArray;
      } else {

          var left = [];
          var right = [];
          var newArray = [];
          var pivot = origArray[origArray.length - 1][0];
          var length = origArray.length;

          for (var i = 0; i < length; i++) {
              if (origArray[i][0] <= pivot) {
                  left.push(origArray[i][0]);
              } else {
                  right.push(origArray[i][0]);
              }
          }

          return newArray.concat(OrdenarPorPucs(left), pivot, OrdenarPorPucs(right));
      }*/
      var countOuter = 0;
      var countInner = 0;
      var countSwap = 0;

      for (var g = 0; g < gaps.length; g++) {
        var gap = gaps[g];
        for (var i = gap; i < array.length; i++) {
          countOuter++;
          var temp = array[i];
          for (var j = i; j >= gap && array[j - gap] > temp; j -= gap) {
            countInner++;
            countSwap++;
            array[j] = array[j - gap];
          }
          array[j] = temp;
        }
      }
      return array;
    }


    function CambiarDataCuenta(ArrData) {
      /* ArrData formato
       * 0. Account ID
       * 1. Debit SA
       * 2. Credit SA
       * 3. Debit Mov
       * 4. Credit Mob
       * 5. Debit Final
       * 6. Credit Final
       * 7. Entity
       */
      var ArrReturn = new Array();
      var cont = 0;

      var str_return = '';

      for (var i = 0; i < ArrData.length; i++) {
        for (var j = 0; j < ArrAccounts.length; j++) {
          if (ArrData[i][0] == ArrAccounts[j][0]) {
            var ArrAux = new Array();

            ArrAux[0] = ArrAccounts[j][8];

            ArrAux[1] = ArrAccounts[j][9];

            ArrAux[2] = ArrData[i][1];

            ArrAux[3] = ArrData[i][2];

            ArrAux[4] = ArrData[i][3];

            ArrAux[5] = ArrData[i][4];

            ArrAux[6] = ArrData[i][5];

            ArrAux[7] = ArrData[i][6];

            ArrAux[8] = ArrAccounts[j][6];

            ArrAux[9] = ArrAccounts[j][7];

            ArrAux[10] = ArrAccounts[j][4];

            ArrAux[11] = ArrAccounts[j][5];

            ArrAux[12] = ArrAccounts[j][2];

            ArrAux[13] = ArrAccounts[j][3];

            ArrAux[14] = ArrData[i][7];

            ArrReturn[cont] = ArrAux;
            cont++;

            //ArrAccounts.splice(j, 1);

            break;
          }
        }
      }

      return ArrReturn;
    }

    function ConvertToArray(strFile) {
      var rows = strFile.split('\r\n');

      var ArrReturn = new Array();
      var cont = 0;

      for (var i = 0; i < rows.length - 1; i++) {
        var columns = rows[i].split('|');

        var arr = new Array();

        for (var j = 0; j < columns.length - 1; j++) {
          arr[j] = columns[j];
        }

        ArrReturn[cont] = arr;
        cont++;
      }

      return ArrReturn;
    }

    function ObtenerFile() {
      var transactionFile = fileModulo.load({
        id: paramFileID
      });

      return transactionFile.getContents();
    }

    function ObtenerParametros() {
      paramFileID = objContext.getParameter({
        name: 'custscript_lmry_co_terce_schdl_fileid'
      });

      paramRecordID = objContext.getParameter({
        name: 'custscript_lmry_co_terce_schdl_recordid'
      });

      paramEntityID = objContext.getParameter({
        name: 'custscript_lmry_co_terce_schdl_entity'
      });

      paramMultibook = objContext.getParameter({
        name: 'custscript_lmry_co_terce_schdl_multi'
      });

      paramSubsidy = objContext.getParameter({
        name: 'custscript_lmry_co_terce_schdl_subsi'
      });

      paramPeriod = objContext.getParameter({
        name: 'custscript_lmry_co_terce_schdl_period'
      });

      paramStep = Number(objContext.getParameter({
        name: 'custscript_lmry_co_terce_schdl_step'
      }));

      paramLastPuc = objContext.getParameter({
        name: 'custscript_lmry_co_terce_schdl_lastpuc'
      });

      paramPeriodFin = objContext.getParameter({
        name: 'custscript_lmry_co_terce_schdl_periodfin'
      });

      paramAdjustment = objContext.getParameter({
        name: 'custscript_lmry_co_terce_schdl_adjust'
      });

      if (paramLastPuc == null) {
        paramLastPuc = 1;
      }

      if (paramStep == null) {
        paramStep = 0;
      }

      //Features
      featuresubs = runtime.isFeatureInEffect({
        feature: "SUBSIDIARIES"
      });

      feamultibook = runtime.isFeatureInEffect({
        feature: "MULTIBOOK"
      });

      var period_temp = search.lookupFields({
        type: search.Type.ACCOUNTING_PERIOD,
        id: paramPeriod,
        columns: ['periodname', 'startdate', 'enddate']
      });

      periodenddate = period_temp.enddate;
      periodstartdate = period_temp.startdate;
      periodname = period_temp.periodname;

      if (paramPeriodFin != null && paramPeriodFin != '') {
        var period_temp_final = search.lookupFields({
          type: search.Type.ACCOUNTING_PERIOD,
          id: paramPeriodFin,
          columns: ['periodname', 'startdate', 'enddate']
        });

        periodnamefinal = period_temp_final.periodname;
      }

      if (feamultibook || feamultibook == 'T') {
        var multibookName_temp = search.lookupFields({
          type: search.Type.ACCOUNTING_BOOK,
          id: paramMultibook,
          columns: ['name']
        });

        multibookName = multibookName_temp.name;
      }
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
          ['isinactive', 'is', 'F'], /*'and',['subsidiary','is',paramSubsidy],*/ 'and', ['custrecord_lmry_co_puc_d6_id', 'isnotempty', '']
        ],
        columns: ['internalid', 'number', 'custrecord_lmry_co_puc_d1_id', 'custrecord_lmry_co_puc_d1_description', 'custrecord_lmry_co_puc_d2_id', 'custrecord_lmry_co_puc_d2_description', 'custrecord_lmry_co_puc_d4_id', 'custrecord_lmry_co_puc_d4_description', 'custrecord_lmry_co_puc_d6_id', 'custrecord_lmry_co_puc_d6_description']
      });

      if (featuresubs) {
        var subsidiaryFilter = search.createFilter({
          name: 'subsidiary',
          operator: search.Operator.IS,
          values: [paramSubsidy]
        });
        busqueda.filters.push(subsidiaryFilter);
      }

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
            //9. puc 6 des
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

    function savefile(Final_string) {
      var FolderId = objContext.getParameter({
        name: 'custscript_lmry_file_cabinet_rg_co'
      });

      // Almacena en la carpeta de Archivos Generados
      if (FolderId != '' && FolderId != null) {
        if (feamultibook || feamultibook == 'T') {
          var Final_NameFile = 'Reporte_Balance_Comprobacion_Terceros_' + paramPeriod + '_' + paramMultibook + '_' + paramLastPuc + '.xls';
        } else {
          var Final_NameFile = 'Reporte_Balance_Comprobacion_Terceros_' + paramPeriod + '_' + paramLastPuc + '.xls';
        }
        // Crea el archivo.xls
        var file = fileModulo.create({
          name: Final_NameFile,
          fileType: fileModulo.Type.EXCEL,
          contents: Final_string,
          folder: FolderId
        });

        var idfile = file.save(); // Termina de grabar el archivo
        var idfile2 = fileModulo.load({
          id: idfile
        }); // Trae URL de archivo generado

        // Obtenemo de las prefencias generales el URL de Netsuite (Produccion o Sandbox)
        var getURL = objContext.getParameter({
          name: 'custscript_lmry_netsuite_location'
        });
        var urlfile = '';

        if (getURL != '' && getURL != '') {
          urlfile += 'https://' + getURL;
        }

        urlfile += idfile2.url;

        var usuarioTemp = runtime.getCurrentUser();
        var usuario = usuarioTemp.name;

        if (paramRecordID != null && paramRecordID != '') {
          var record = recordModulo.load({
            type: 'customrecord_lmry_co_rpt_generator_log',
            id: paramRecordID
          });
        } else {
          var record = recordModulo.create({
            type: 'customrecord_lmry_co_rpt_generator_log'
          });
        }

        //Nombre de Archivo
        record.setValue({
          fieldId: 'custrecord_lmry_co_rg_name',
          value: Final_NameFile
        });

        //Url de Archivo
        record.setValue({
          fieldId: 'custrecord_lmry_co_rg_url_file',
          value: urlfile
        });

        //Nombre de Reporte
        record.setValue({
          fieldId: 'custrecord_lmry_co_rg_transaction',
          value: namereport
        });

        //Nombre de Subsidiaria
        record.setValue({
          fieldId: 'custrecord_lmry_co_rg_subsidiary',
          value: companyname
        });

        //Periodo
        if (periodnamefinal != null && periodnamefinal != '') {
          record.setValue({
            fieldId: 'custrecord_lmry_co_rg_postingperiod',
            value: periodname + ' - ' + periodnamefinal
          });
        } else {
          record.setValue({
            fieldId: 'custrecord_lmry_co_rg_postingperiod',
            value: periodname
          });
        }

        //Multibook
        if (feamultibook || feamultibook == 'T') {
          record.setValue({
            fieldId: 'custrecord_lmry_co_rg_multibook',
            value: multibookName
          });
        }

        if (paramEntityID != null) {
          record.setValue({
            fieldId: 'custrecord_lmry_co_rg_entity',
            value: entity_name
          });
        }

        //Creado Por
        record.setValue({
          fieldId: 'custrecord_lmry_co_rg_employee',
          value: usuario
        });

        record.save();
        libreria.sendrptuser(namereport, 3, Final_NameFile);

        return idfile;
      }
    }

    function ValidarAcentos(s) {
      var AccChars = "ŠŽšžŸÀÁÂÃÄÅÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖÙÚÛÜÝàáâãäåçèéêëìíîïðñòóôõöùúûüýÿ°–—ªº·&_!¡";
      var RegChars = "SZszYAAAAAACEEEEIIIIDNOOOOOUUUUYaaaaaaceeeeiiiidnooooouuuuyyo--ao.y   ";
      s = s.toString();
      for (var c = 0; c < s.length; c++) {
        for (var special = 0; special < AccChars.length; special++) {
          if (s.charAt(c) == AccChars.charAt(special)) {
            s = s.substring(0, c) + RegChars.charAt(special) + s.substring(c + 1, s.length);
          }
        }
      }
      return s;
    }

    function ObtenerEntidad(paramEntity) {
      if (paramEntity != null && paramEntity != '') {
        var entity_customer_temp = search.lookupFields({
          type: search.Type.CUSTOMER,
          id: Number(paramEntity),
          columns: ['entityid', 'firstname', 'lastname', 'companyname', 'internalid', 'vatregnumber', 'custentity_lmry_digito_verificator']
        });

        var entity_id;

        entity_nit = entity_customer_temp.vatregnumber + entity_customer_temp.custentity_lmry_digito_verificator;

        if (entity_customer_temp.internalid != null) {
          entity_id = (entity_customer_temp.internalid)[0].value;
        }

        entity_name = entity_customer_temp.firstname + ' ' + entity_customer_temp.lastname;

        if ((entity_customer_temp.firstname == null || entity_customer_temp.firstname == '') && (entity_customer_temp.lastname == null || entity_customer_temp.lastname == '') && entity_name.trim() == '') {
          entity_name = entity_customer_temp.companyname;

          if (entity_name == null && entity_name.trim() == '') {
            entity_name = entity_customer_temp.entityid;
          }
        }

        if (entity_id != null) {
          entityCustomer = true;
          return true;
        } else {
          var entity_vendor_temp = search.lookupFields({
            type: search.Type.VENDOR,
            id: paramEntity,
            columns: ['entityid', 'firstname', 'lastname', 'companyname', 'internalid', 'vatregnumber', 'custentity_lmry_digito_verificator']
          });

          entity_nit = entity_vendor_temp.vatregnumber + entity_vendor_temp.custentity_lmry_digito_verificator;

          if (entity_vendor_temp.internalid != null) {
            entity_id = (entity_vendor_temp.internalid)[0].value;
          }

          entity_name = entity_vendor_temp.firstname + ' ' + entity_vendor_temp.lastname;

          if ((entity_vendor_temp.firstname == null || entity_vendor_temp.firstname == '') && (entity_vendor_temp.lastname == null || entity_vendor_temp.lastname == '') && entity_name.trim() == '') {
            entity_name = entity_vendor_temp.companyname;

            if (entity_name == null && entity_name.trim() == '') {
              entity_name = entity_vendor_temp.entityid;
            }
          }

          if (entity_id != null) {
            entityVendor = true;
            return true;
          } else {
            var entity_employee_temp = search.lookupFields({
              type: search.Type.EMPLOYEE,
              id: paramEntity,
              columns: ['entityid', 'firstname', 'lastname', 'internalid', 'custentity_lmry_sv_taxpayer_number', 'custentity_lmry_digito_verificator']
            });

            entity_nit = entity_employee_temp.custentity_lmry_sv_taxpayer_number + entity_employee_temp.custentity_lmry_digito_verificator;

            if (entity_employee_temp.internalid != null) {
              entity_id = (entity_employee_temp.internalid)[0].value;
            }

            entity_name = entity_employee_temp.firstname + ' ' + entity_employee_temp.lastname;

            if (entity_name == null && entity_name.trim() == '') {
              entity_name = entity_employee_temp.entityid;
            }

            if (entity_id != null) {
              entityEmployee = true;
              return true;
            } else {
              var otherNameRcd = recordModulo.load({
                type: search.Type.OTHER_NAME,
                id: paramEntity
              });

              var entityidField = otherNameRcd.getValue({
                fieldId: 'entityid'
              });

              var vatregnumberField = otherNameRcd.getValue({
                fieldId: 'vatregnumber'
              });

              var ispersonField = otherNameRcd.getValue({
                fieldId: 'isperson'
              });

              var firstnameField = otherNameRcd.getValue({
                fieldId: 'firstname'
              });

              var lastnameField = otherNameRcd.getValue({
                fieldId: 'lastname'
              });

              var companynameField = otherNameRcd.getValue({
                fieldId: 'companyname'
              });

              var internalidField = otherNameRcd.getValue({
                fieldId: 'id'
              });

              entity_nit = vatregnumberField;

              if (internalidField != null) {
                entity_id = internalidField;
              }

              if (ispersonField == true || ispersonField == 'T') {
                entity_name = firstnameField + ' ' + lastnameField;
              } else {
                entity_name = companynameField;
              }

              if (entity_id != null) {
                entityOtherName = true;
                return true;
              } else {
                return false;
              }
            }
          }
        }
      } else {
        return false;
      }
    }

    return {
      execute: execute
    };

  });
