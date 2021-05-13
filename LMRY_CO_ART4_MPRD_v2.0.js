/* = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =\
||   This script for customer center (Time)                     ||
||                                                              ||
||  File Name: LMRY_CO_ART4_MPRD_v2.0.js                        ||
||                                                              ||
||  Version Date         Author        Remarks                  ||
||  2.0     Sep 04 2020  LatamReady    Use Script 2.0           ||
\= = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = */
/**
 * @NApiVersion 2.x
 * @NScriptType MapReduceScript
 * @NModuleScope Public
 */
define(['N/search', 'N/log', "N/config", 'require', 'N/file', 'N/runtime', 'N/query', "N/format", "N/record", "N/task", "./CO_Library_Mensual/LMRY_CO_Reportes_LBRY_V2.0.js"],

  function(search, log, config, require, fileModulo, runtime, query, format, recordModulo, task, libreria) {

    /**
     * Input Data for processing
     *
     * @return Array,Object,Search,File
     *
     * @since 2016.1
     */

    var objContext = runtime.getCurrentScript();

    var LMRY_script = "LMRY_CO_ART4_MPRD_v2.0.js";

    //Parametros
    var param_RecorID = objContext.getParameter({
      name: 'custscript_lmry_co_art4_recordid'
    });
    var param_Periodo = objContext.getParameter({
      name: 'custscript_lmry_co_art4_periodo'
    });
    var param_Anual = objContext.getParameter({
      name: 'custscript_lmry_co_art4_periodo_anual'
    });
    var param_Multi = objContext.getParameter({
      name: 'custscript_lmry_co_art4_multibook'
    });
    var param_Feature = objContext.getParameter({
      name: 'custscript_lmry_co_art4_feature'
    });
    var param_Subsi = objContext.getParameter({
      name: 'custscript_lmry_co_art4_subsidiaria'
    });
    var param_Titulo = objContext.getParameter({
      name: 'custscript_lmry_co_art4_cabecera'
    });

    //************FEATURES********************
    var feature_Subsi = runtime.isFeatureInEffect({
      feature: "SUBSIDIARIES"
    });

    var feature_Multi = runtime.isFeatureInEffect({
      feature: "MULTIBOOK"
    });

    var language = runtime.getCurrentScript().getParameter({
      name: 'LANGUAGE'
    }).substring(0, 2);

    function getInputData() {
      try {
        log.debug('empezo a correr la bsuqueda', param_Multi + '-' + param_Subsi + '-' + param_Periodo + '-' + param_Anual);

        var whtLines = getLinesWHT();
        log.debug('whtLines', whtLines);
        var whtTotal = getTotalWHT();
        var whtJournalLines = getJournalWHT();
        log.debug('whtJournalLines', whtJournalLines);
        //recuerda retornar el arreglosgaaaaa
        var ArrReturn = whtLines.concat(whtTotal, whtJournalLines);
        log.error('valor del arreglo de retorno', ArrReturn);
        return ArrReturn;

      } catch (err) {
        log.error('err', err);
        //libreria.sendMail(LMRY_script, ' [ getInputData ] ' + err);
      }
    }

    /**
     * If this entry point is used, the map function is invoked one time for each key/value.
     *
     * @param {Object} context
     * @param {boolean} context.isRestarted - Indicates whether the current invocation represents a restart
     * @param {number} context.executionNo - Version of the bundle being installed
     * @param {Iterator} context.errors - This param contains a "iterator().each(parameters)" function
     * @param {string} context.key - The key to be processed during the current invocation
     * @param {string} context.value - The value to be processed during the current invocation
     * @param {function} context.write - This data is passed to the reduce stage
     *
     * @since 2016.1
     */
    function map(context) {
      try {
        var arrTemp = JSON.parse(context.value);
        var dataVendor = new Array();
        var montoBase = 0;
        var alicuota = 0;
        var montoRetencion = 0;
        var id_reduce;

        if (arrTemp[0] == 'Journal') {
          //log.debug('entro a journal map');
          var vendorEntity = getVendorData(arrTemp[3]);

          if (vendorEntity != null) {
            //log.debug('viene de journal');
            var vendorDetailData = getVendorAddressData(arrTemp[3]);
            var taxResults = getTaxResults(arrTemp[1], arrTemp[2]);

            if (taxResults.length != 0) {
              log.debug('esta es', arrTemp);
              dataVendor = [vendorEntity[0], vendorEntity[4], vendorEntity[1], vendorEntity[2],
              vendorDetailData[0], vendorDetailData[1], vendorDetailData[2], vendorEntity[3]];

              montoBase = taxResults[0][0];
              alicuota = taxResults[0][2];
              montoRetencion = taxResults[0][1];

              id_reduce = arrTemp[3] + '|' + alicuota; //ID VENDOR + ALIQUOTA
            }else{
              log.debug('No hay taxresult en journal', vendorEntity);
              log.debug('No hay taxresult con esto', arrTemp);
              return false;
            }
          }else{
            return false;
          }

        } else {
          var datos = getVendorAddressData(arrTemp[3]);
          var vendorEntity = getVendorData(arrTemp[3]);//para obtener Cod. Documento

          dataVendor = [arrTemp[0], vendorEntity[4], arrTemp[2], arrTemp[4], datos[0], datos[1], datos[2], arrTemp[8]];
          montoBase = arrTemp[5];
          alicuota = arrTemp[7];
          montoRetencion = arrTemp[6];

          id_reduce = arrTemp[3] + '|' + arrTemp[7]; //ID VENDOR + ALIQUOTA
        }
        log.debug('id_reduce',id_reduce);

        context.write({
          key: id_reduce,
          value: {
            Vendor: dataVendor,
            Montobase: montoBase,
            Aliquota: alicuota,
            MontoRetenido: montoRetencion
          }
        });

      } catch (err) {
        log.error('err', err);
      }
    }

    /**
     * If this entry point is used, the reduce function is invoked one time for
     * each key and list of values provided..
     *
     * @param {Object} context
     * @param {boolean} context.isRestarted - Indicates whether the current invocation of the represents a restart.
     * @param {number} context.concurrency - The maximum concurrency number when running the map/reduce script.
     * @param {Date} 0context.datecreated - The time and day when the script began running.
     * @param {number} context.seconds - The total number of seconds that elapsed during the processing of the script.
     * @param {number} context.usage - TThe total number of usage units consumed during the processing of the script.
     * @param {number} context.yields - The total number of yields that occurred during the processing of the script.
     * @param {Object} context.inputSummary - Object that contains data about the input stage.
     * @param {Object} context.mapSummary - Object that contains data about the map stage.
     * @param {Object} context.reduceSummary - Object that contains data about the reduce stage.
     * @param {Iterator} context.output - This param contains a "iterator().each(parameters)" function
     *
     * @since 2016.1
     */

    function reduce(context) {
      var ArrVendor = new Array();
      var monto_B = 0;
      var monto_R = 0;
      var por = '';
      var arreglo = context.values;
      // log.error('deberia de venir agrupado',arreglo);
      var tamaño = arreglo.length;
      for (var i = 0; i < tamaño; i++) {
        var obj = JSON.parse(arreglo[i]);

        ArrVendor = obj.Vendor;
        monto_B += obj.Montobase;
        monto_R += obj.MontoRetenido;
        por = Number(obj.Aliquota);
        por = por.toFixed(2);
      }
      monto_B = redondear(monto_B);
      monto_R = redondear(monto_R);
      /*log.debug('vendor', ArrVendor);
      log.debug('monto base sumado', monto_B);
      log.debug('porcentae', por);
      log.debug('monto retenido sumado', monto_R);*/

      context.write({
        key: context.key,
        value: {
          Vendor: ArrVendor,
          Montobase: monto_B,
          Aliquota: por,
          MontoRetenido: monto_R
        }
      });

    }

    function summarize(context) {

      try {
        strReporte = '';
        //para obtener el año de generacion de reporte
        if (param_Periodo != '' && param_Periodo != null) {
          var periodenddate_temp = search.lookupFields({
            type: search.Type.ACCOUNTING_PERIOD,
            id: param_Anual,
            columns: ['periodname']
          });
          //Period EndDate
          Anual = periodenddate_temp.periodname;
          Anual = Anual.substring(Anual.length - 4, Anual.length);
          log.error('nombre del año', Anual);
          periodname = periodenddate_temp.periodname;

        } else {
          var periodenddate_temp = search.lookupFields({
            type: search.Type.ACCOUNTING_PERIOD,
            id: param_Periodo,
            columns: ['enddate', 'periodname']
          });
          //Period EndDate
          periodenddate = periodenddate_temp.enddate;
          var parsedDateStringAsRawDateObject = format.parse({
            value: periodenddate,
            type: format.Type.DATE
          });

          periodname = periodenddate_temp.periodname;
          var Anual = parsedDateStringAsRawDateObject.getFullYear();
          log.error('valor del año', Anual);
        }
        var salto = '\r\n';
        context.output.iterator().each(function(key, value) {
          var obj = JSON.parse(value);
          if (obj["isError"] == "T") {
            errores.push(JSON.stringify(obj["error"]));
          } else {
            ArrVendor = obj.Vendor;
            //log.error('quiero ver como viene',ArrVendor);
            monto_base = obj.Montobase;
            MontoRet = obj.MontoRetenido;
            porc = obj.Aliquota;
            //log.error('param_Titulo',param_Titulo);
            if (param_Titulo == 'T' || param_Titulo == true) {
              strReporte += Anual + ',' + ArrVendor[1] + ',' + ArrVendor[2] + ',' + ArrVendor[0] + ',' + ArrVendor[4] + ',' + ArrVendor[7] + ',' + ArrVendor[3] + ',' + ArrVendor[5] + ',' + ArrVendor[6] + ',' + monto_base + ',' + porc + ',' + MontoRet + salto;
            } else {
              strReporte += Anual + ';' + ArrVendor[1] + ';' + ArrVendor[2] + ';' + ArrVendor[0] + ';' + ArrVendor[4] + ';' + ArrVendor[7] + ';' + ArrVendor[3] + ';' + ArrVendor[5] + ';' + ArrVendor[6] + ';' + monto_base + ';' + porc + ';' + MontoRet + salto;
            }


          }
          return true;
        });
        log.error('strReporte', strReporte);

        //obtener nombre de subsidiaria
        var configpage = config.load({
          type: config.Type.COMPANY_INFORMATION
        });

        if (feature_Subsi) {
          companyname = ObtainNameSubsidiaria(param_Subsi);
          companyname = validarAcentos(companyname);
          companyruc = ObtainFederalIdSubsidiaria(param_Subsi);
        } else {
          companyruc = configpage.getValue('employerid');
          companyname = configpage.getValue('legalname');
        }

        companyruc = companyruc.replace(' ', '');
        companyruc = ValidaGuion(companyruc);

        if (strReporte == '') {
          NoData(periodname);
          return true;
        }

        var folderId = objContext.getParameter({
          name: 'custscript_lmry_file_cabinet_rg_co'
        });

        // Almacena en la carpeta de Archivos Generados
        if (folderId != '' && folderId != null) {
          // Extension del archivo
          if (param_Titulo == 'T' || param_Titulo == true) {
            var fileExt = '.csv';
            var nameFile = NameFile(companyruc, Anual) + fileExt;
            var titulo = 'VIGENCIA' + ',' + 'TIPO DOCUMENTO' + ',' + 'NUMERO DE DOCUMENTO' + ',' + 'NOMBRE O RAZON SOCIAL' + ',' + 'DIRECCION DE NOTIFICACION' + ',' + 'TELEFONO' + ',' + 'E-MAIL' + ',' + 'CODIGO MUNICIPIO' + ',' + 'CODIGO DEPTO.' + ',' + 'BASE RETENCION' + ',' + 'TARIFA RETENCION APLICADA' + ',' + 'MONTO RETENCION ANUAL' + '\r\n';
            strReporte = titulo + strReporte;

            // Crea el archivo
            var reportFile = fileModulo.create({
              name: nameFile,
              fileType: fileModulo.Type.CSV,
              contents: strReporte,
              encoding: fileModulo.Encoding.ISO_8859_1,
              folder: folderId
            });
          } else {
            var fileExt = '.txt';
            var nameFile = NameFile(companyruc, Anual) + fileExt;

            // Crea el archivo
            var reportFile = fileModulo.create({
              name: nameFile,
              fileType: fileModulo.Type.PLAINTEXT,
              contents: strReporte,
              encoding: fileModulo.Encoding.ISO_8859_1,
              folder: folderId
            });
          }

          var idFile = reportFile.save();

          var idfile2 = fileModulo.load({
            id: idFile
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

          log.debug({
            title: 'url',
            details: urlfile
          });

          //Genera registro personalizado como log

          var nombre = search.lookupFields({
            type: "customrecord_lmry_co_features",
            id: param_Feature,
            columns: ['name']
          });
          namereport = nombre.name;

          if (idFile) {
            var usuarioTemp = runtime.getCurrentUser();
            var id = usuarioTemp.id;
            var employeename = search.lookupFields({
              type: search.Type.EMPLOYEE,
              id: id,
              columns: ['firstname', 'lastname']
            });
            var usuario = employeename.firstname + ' ' + employeename.lastname;

            if (false) {
              var record = recordModulo.create({
                type: 'customrecord_lmry_co_rpt_generator_log',
              });
              //Nombre de Reporte
              record.setValue({
                fieldId: 'custrecord_lmry_co_rg_transaction',
                value: 'CO - Art 4'
              });
              //Nombre de Subsidiaria
              record.setValue({
                fieldId: 'custrecord_lmry_co_rg_subsidiary',
                value: companyname
              });
              //Multibook
              record.setValue({
                fieldId: 'custrecord_lmry_co_rg_multibook',
                value: multibookName
              });

            } else {
              log.debug('Carga Linea de LOG');
              var record = recordModulo.load({
                type: 'customrecord_lmry_co_rpt_generator_log',
                id: param_RecorID
              });

            }

            //Nombre de Archivo
            record.setValue({
              fieldId: 'custrecord_lmry_co_rg_name',
              value: nameFile
            });
            //Url de Archivo
            record.setValue({
              fieldId: 'custrecord_lmry_co_rg_url_file',
              value: urlfile
            });
            //Periodo
            record.setValue({
              fieldId: 'custrecord_lmry_co_rg_postingperiod',
              value: periodname
            });
            //Creado Por
            record.setValue({
              fieldId: 'custrecord_lmry_co_rg_employee',
              value: usuario
            });

            var recordId = record.save();
            // Envia mail de conformidad al usuario
            //libreria.sendrptuserTranslate(namereport, 3, NameFile, language);
          }
        }
        log.debug('paso de guardar el archivo', idFile);
      } catch (err) {
        log.error('err', err);
        //libreria.sendMail(LMRY_script, ' [ getInputData ] ' + err);
      }
    }

    function getLinesWHT() {
      var intDMinReg = 0;
      var intDMaxReg = 1000;
      var DbolStop = false;
      //para la busqueda de transacciones
      var arrResult = new Array();

      var savedsearch = search.load({
        /*LatamReady - CO ART4 Transaccion*/
        id: 'customsearch_lmry_co_art4_transacciones'
      });

      if (feature_Subsi) {
        var subsidiaryFilter = search.createFilter({
          name: 'subsidiary',
          operator: search.Operator.IS,
          values: [param_Subsi]
        });
        savedsearch.filters.push(subsidiaryFilter);
      }

      if (param_Anual != null && param_Anual != '') {
        var periodFilter = search.createFilter({
          name: 'postingperiod',
          operator: search.Operator.IS,
          values: [param_Anual]
        });
        savedsearch.filters.push(periodFilter);
      } else {
        var periodFilter = search.createFilter({
          name: 'postingperiod',
          operator: search.Operator.IS,
          values: [param_Periodo]
        });
        savedsearch.filters.push(periodFilter);
      }
      var searchResult = savedsearch.run();
      //para usarlos en los formatos
      var auxiliar = '';
      while (!DbolStop) {
        var objResult = searchResult.getRange(intDMinReg, intDMaxReg);
        log.error('tamaño de la busqueda', objResult.length);
        if (objResult != null) {
          if (objResult.length != 1000) {
            DbolStop = true;
          }

          for (var i = 0; i < objResult.length; i++) {
            var columns = objResult[i].columns;
            var arrAuxiliar = new Array();
            // 0. nombre
            if (objResult[i].getValue(columns[0]) != '' && objResult[i].getValue(columns[0]) != null && objResult[i].getValue(columns[0]) != '- None -') {
              arrAuxiliar[0] = objResult[i].getValue(columns[0]);
              arrAuxiliar[0] = ValidarCaracteres_Especiales(arrAuxiliar[0]);
              arrAuxiliar[0] = Valida_colombia(arrAuxiliar[0]);
              arrAuxiliar[0] = arrAuxiliar[0].substring(0, 70);
            } else {
              arrAuxiliar[0] = '';
            }
            //1. tipo documento
            if (objResult[i].getValue(columns[1]) != '' && objResult[i].getValue(columns[1]) != null && objResult[i].getValue(columns[1]) != '- None -') {
              arrAuxiliar[1] = objResult[i].getValue(columns[1]);
            } else {
              arrAuxiliar[1] = '';
            }
            //2.codigo
            if (objResult[i].getValue(columns[2]) != '' && objResult[i].getValue(columns[2]) != null && objResult[i].getValue(columns[2]) != '- None -') {
              arrAuxiliar[2] = objResult[i].getValue(columns[2]);
              arrAuxiliar[2] = Valida_Codigo(arrAuxiliar[2]);
              arrAuxiliar[2] = arrAuxiliar[2].substring(0, 11);

            } else {
              arrAuxiliar[2] = '';
            }
            //3.id vendor
            arrAuxiliar[3] = objResult[i].getValue(columns[3]);
            //4.email
            if (objResult[i].getValue(columns[4]) != '' && objResult[i].getValue(columns[4]) != null && objResult[i].getValue(columns[4]) != '- None -') {
              arrAuxiliar[4] = objResult[i].getValue(columns[4]);
              arrAuxiliar[4] = arrAuxiliar[4].substring(0, 70);
            } else {
              arrAuxiliar[4] = '';
            }
            //5.MULTIBOOK
            if (objResult[i].getValue(columns[8]) != null && objResult[i].getValue(columns[8]) != '- None -') {
              var exch_rate_nf = objResult[i].getValue(columns[8]);
              exch_rate_nf = exchange_rate(exch_rate_nf);
            } else {
              exch_rate_nf = 1;
            }

            var typeTransaction = objResult[i].getValue(columns[10]);
            var factorSigno = 1;
            if (typeTransaction == 'VendCred') {//Bill Credit (Devolucion de Retencion)
              factorSigno = -1;
            }

            //6. monto base
            arrAuxiliar[5] = objResult[i].getValue(columns[5]) * exch_rate_nf * factorSigno;
            //7. monto retenido
            arrAuxiliar[6] = objResult[i].getValue(columns[7]) * exch_rate_nf * factorSigno;
            //8. porcentaje
            arrAuxiliar[7] = Number(objResult[i].getValue(columns[6])) * 10000;
            //9. telefono
            if (objResult[i].getValue(columns[9]) != '' && objResult[i].getValue(columns[9]) != null && objResult[i].getValue(columns[9]) != '- None -') {
              arrAuxiliar[8] = objResult[i].getValue(columns[9]);
              arrAuxiliar[8] = ValidaGuion(arrAuxiliar[8]);
              arrAuxiliar[8] = arrAuxiliar[8].substring(0, 10)
            } else {
              arrAuxiliar[8] = '';
            }
            //LLenamos los valores en el arreglo
            arrResult.push(arrAuxiliar);
          }
          if (!DbolStop) {
            intDMinReg = intDMaxReg;
            intDMaxReg += 1000;
          }
        } else {
          DbolStop = true;
        }
      }

      return arrResult;
    }

    function getTotalWHT() {
      var arrResult = [];

      var savedsearch_2 = search.load({
        /*LatamReady - CO ART4 Totales */
        id: 'customsearch_lmry_co_art4_totales'
      });

      if (feature_Subsi) {
        var subsidiaryFilter = search.createFilter({
          name: 'subsidiary',
          operator: search.Operator.IS,
          values: [param_Subsi]
        });
        savedsearch_2.filters.push(subsidiaryFilter);
      }

      if (param_Anual != null && param_Anual != '') {
        var periodFilter = search.createFilter({
          name: 'postingperiod',
          operator: search.Operator.IS,
          values: [param_Anual]
        });
        savedsearch_2.filters.push(periodFilter);
      } else {
        var periodFilter = search.createFilter({
          name: 'postingperiod',
          operator: search.Operator.IS,
          values: [param_Periodo]
        });
        savedsearch_2.filters.push(periodFilter);
      }
      //12
      var columna_tipo_rete = search.createColumn({
        name: "custrecord_lmry_wht_salebase",
        join: "CUSTBODY_LMRY_CO_RETEICA",
        summary: "GROUP",
        label: "Sale WHT Base"
      });
      savedsearch_2.columns.push(columna_tipo_rete);
      //13
      var exchangerate = search.createColumn({
        name: "exchangerate",
        summary: "GROUP",
        label: "Exchange Rate"
      });
      savedsearch_2.columns.push(exchangerate);

      if (feature_Multi) {
        var multibookFilter = search.createFilter({
          name: 'accountingbook',
          join: 'accountingtransaction',
          operator: search.Operator.IS,
          values: [param_Multi]
        });
        savedsearch_2.filters.push(multibookFilter);
        //14
        var exchange_rate_multi = search.createColumn({
          name: "exchangerate",
          join: "accountingTransaction",
          summary: "GROUP",
          label: "Exchange Rate"
        });
        savedsearch_2.columns.push(exchange_rate_multi);
      }

      var searchResult = savedsearch_2.run();
      intDMinReg = 0;
      intDMaxReg = 1000;
      DbolStop = false;

      while (!DbolStop) {
        var objResult = searchResult.getRange(intDMinReg, intDMaxReg);
        log.error('tamaño de la busqueda', objResult.length);
        if (objResult != null) {
          if (objResult.length != 1000) {
            DbolStop = true;
          }

          for (var i = 0; i < objResult.length; i++) {
            var columns = objResult[i].columns;
            var arrAuxiliar = new Array();
            // 0. nombre
            if (objResult[i].getValue(columns[0]) != '' && objResult[i].getValue(columns[0]) != null && objResult[i].getValue(columns[0]) != '- None -') {
              arrAuxiliar[0] = objResult[i].getValue(columns[0]);
              arrAuxiliar[0] = ValidarCaracteres_Especiales(arrAuxiliar[0]);
              arrAuxiliar[0] = Valida_colombia(arrAuxiliar[0]);
              arrAuxiliar[0] = arrAuxiliar[0].substring(0, 70);
            } else {
              arrAuxiliar[0] = '';
            }
            //1. tipo documento
            if (objResult[i].getValue(columns[1]) != '' && objResult[i].getValue(columns[1]) != null && objResult[i].getValue(columns[1]) != '- None -') {
              arrAuxiliar[1] = objResult[i].getValue(columns[1]);
            } else {
              arrAuxiliar[1] = '';
            }
            //2.codigo
            if (objResult[i].getValue(columns[2]) != '' && objResult[i].getValue(columns[2]) != null && objResult[i].getValue(columns[2]) != '- None -') {
              arrAuxiliar[2] = objResult[i].getValue(columns[2]);
              arrAuxiliar[2] = Valida_Codigo(arrAuxiliar[2]);
              arrAuxiliar[2] = arrAuxiliar[2].substring(0, 11);

            } else {
              arrAuxiliar[2] = '';
            }
            //3.id vendor
            arrAuxiliar[3] = objResult[i].getValue(columns[3]);
            //4.email
            if (objResult[i].getValue(columns[4]) != '' && objResult[i].getValue(columns[4]) != null && objResult[i].getValue(columns[4]) != '- None -') {
              arrAuxiliar[4] = objResult[i].getValue(columns[4]);
              arrAuxiliar[4] = arrAuxiliar[4].substring(0, 70);
            } else {
              arrAuxiliar[4] = '';
            }
            //id de retencion
            //log.error('no lo veo', objResult[i].getValue(columns[12]));
            if (objResult[i].getValue(columns[12]) != null && objResult[i].getValue(columns[12]) != '- None -') {
              id_retencion = objResult[i].getValue(columns[12]);
            } else {
              id_retencion = '';
            }

            var typeTransaction = objResult[i].getValue(columns[11]);
            var factorSigno = 1;
            if (typeTransaction == 'VendCred') {//Bill Credit (Devolucion de Retencion)
              factorSigno = -1;
            }

            //6. monto base
            if (feature_Multi) {
              if (id_retencion == 1) {
                arrAuxiliar[5] = Number(objResult[i].getValue(columns[6])) / Number(objResult[i].getValue(columns[13])) * Number(objResult[i].getValue(columns[14]));
              } else if (id_retencion == 2) {
                arrAuxiliar[5] = Number(objResult[i].getValue(columns[8])) / Number(objResult[i].getValue(columns[13])) * Number(objResult[i].getValue(columns[14]));
              } else if (id_retencion == 3) {
                arrAuxiliar[5] = Number(objResult[i].getValue(columns[7])) / Number(objResult[i].getValue(columns[13])) * Number(objResult[i].getValue(columns[14]));
              }
            } else {
              if (id_retencion == 1) {
                arrAuxiliar[5] = Number(objResult[i].getValue(columns[6])); //)/Number(objResult[i].getValue(columns[13]))*Number(objResult[i].getValue(columns[14]));
              } else if (id_retencion == 2) {
                arrAuxiliar[5] = Number(objResult[i].getValue(columns[8])); //)/Number(objResult[i].getValue(columns[13]))*Number(objResult[i].getValue(columns[14]));
              } else if (id_retencion == 3) {
                arrAuxiliar[5] = Number(objResult[i].getValue(columns[7])); //)/Number(objResult[i].getValue(columns[13]))*Number(objResult[i].getValue(columns[14]));
              }
            }
            arrAuxiliar[5] = arrAuxiliar[5] * factorSigno;

            //7. monto retenido
            arrAuxiliar[6] = Number(objResult[i].getValue(columns[9])) * factorSigno;
            //8. porcentaje
            arrAuxiliar[7] = objResult[i].getValue(columns[10]);
            //5. telefono
            if (objResult[i].getValue(columns[5]) != '' && objResult[i].getValue(columns[5]) != null && objResult[i].getValue(columns[5]) != '- None -') {
              arrAuxiliar[8] = objResult[i].getValue(columns[5]);
              arrAuxiliar[8] = ValidaGuion(arrAuxiliar[8]);
              arrAuxiliar[8] = arrAuxiliar[8].substring(0, 10)
            } else {
              arrAuxiliar[8] = '';
            }
            //LLenamos los valores en el arreglo
            arrResult.push(arrAuxiliar);
          }
          if (!DbolStop) {
            intDMinReg = intDMaxReg;
            intDMaxReg += 1000;
          }
        } else {
          DbolStop = true;
        }
      }

      return arrResult;
    }

    function getJournalWHT() {
      var arrResult = [];

      var savedsearch = search.load({
        /*LatamReady - CO ART4 WHT Journal*/
        id: 'customsearch_lmry_co_art4_wht_journal'
      });

      if (feature_Subsi) {
        var subsidiaryFilter = search.createFilter({
          name: 'subsidiary',
          operator: search.Operator.IS,
          values: [param_Subsi]
        });
        savedsearch.filters.push(subsidiaryFilter);
      }

      if (param_Anual != null && param_Anual != '') {
        var periodFilter = search.createFilter({
          name: 'postingperiod',
          operator: search.Operator.IS,
          values: [param_Anual]
        });
        savedsearch.filters.push(periodFilter);
      } else {
        var periodFilter = search.createFilter({
          name: 'postingperiod',
          operator: search.Operator.IS,
          values: [param_Periodo]
        });
        savedsearch.filters.push(periodFilter);
      }

      if (feature_Multi) {
        var multibookFilter = search.createFilter({
          name: 'accountingbook',
          join: 'accountingtransaction',
          operator: search.Operator.IS,
          values: [param_Multi]
        });
        savedsearch.filters.push(multibookFilter);
      }

      var searchResult = savedsearch.run();
      intDMinReg = 0;
      intDMaxReg = 1000;
      DbolStop = false;

      while (!DbolStop) {
        var objResult = searchResult.getRange(intDMinReg, intDMaxReg);
        //log.debug('tamaño de la busqueda', objResult.length);
        if (objResult != null) {

          if (objResult.length != 1000) {
            DbolStop = true;
          }

          for (var i = 0; i < objResult.length; i++) {
            var columns = objResult[i].columns;
            var arrAuxiliar = new Array();

            for (var j = 0; j < columns.length; j++) {
              arrAuxiliar[j] = objResult[i].getValue(columns[j]);
            }
            //LLenamos los valores en el arreglo
            arrResult.push(arrAuxiliar);
          }

          if (!DbolStop) {
            intDMinReg = intDMaxReg;
            intDMaxReg += 1000;
          }
        } else {
          DbolStop = true;
        }
      }
      return arrResult;
    }

    function NoData(periodname) {
      var usuarioTemp = runtime.getCurrentUser();
      var id = usuarioTemp.id;
      var employeename = search.lookupFields({
        type: search.Type.EMPLOYEE,
        id: id,
        columns: ['firstname', 'lastname']
      });
      var usuario = employeename.firstname + ' ' + employeename.lastname;


      var message = "No existe informacion para los criterios seleccionados.";


      var record = recordModulo.load({
        type: 'customrecord_lmry_co_rpt_generator_log',
        id: param_RecorID
      });

      //Nombre de Archivo
      record.setValue({
        fieldId: 'custrecord_lmry_co_rg_name',
        value: message
      });
      //Periodo
      record.setValue({
        fieldId: 'custrecord_lmry_co_rg_postingperiod',
        value: periodname
      });

      //Creado Por
      record.setValue({
        fieldId: 'custrecord_lmry_co_rg_employee',
        value: usuario
      });

      var recordId = record.save();
    }

    function redondear(number) {
      return Math.round(Number(number));
    }

    function ValidaGuion(s) {
      var AccChars = "+./-[] (),";
      var RegChars = "";
      s = String(s);
      for (var c = 0; c < s.length; c++) {
        for (var special = 0; special < AccChars.length; special++) {
          if (s.charAt(c) == AccChars.charAt(special)) {
            s = s.substring(0, c) + RegChars.charAt(special) + s.substring(c + 1, s.length);
          }
        }
      }
      return s;
    }

    function ValidarCaracteres_Especiales(s) {
      var AccChars = "ŠŽšžŸÀÁÂÃÄÅÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖÙÚÛÜÝàáâãäåçèéêëìíîïðñòóôõöùúûüýÿ°Ñ–—·";
      var RegChars = "SZszYAAAAAACEEEEIIIIDNOOOOOUUUUYaaaaaaceeeeiiiidnooooouuuuyyoN--.";
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

    function Valida_colombia(s) {
      var AccChars = "!“#$%&/()=\\+/*ªº.,;ªº-+_?¿®©";
      var RegChars = "                             ";
      s = String(s);
      for (var c = 0; c < s.length; c++) {
        for (var special = 0; special < AccChars.length; special++) {
          if (s.charAt(c) == AccChars.charAt(special)) {
            s = s.substring(0, c) + RegChars.charAt(special) + s.substring(c + 1, s.length);
          }
        }
      }
      return s;
    }

    function Valida_Codigo(s) {
      var AccChars = "!“#$%&/()=\\+/*ªº.,;ªº-+_";
      var RegChars = "";
      s = String(s);
      for (var c = 0; c < s.length; c++) {
        for (var special = 0; special < AccChars.length; special++) {
          if (s.charAt(c) == AccChars.charAt(special)) {
            s = s.substring(0, c) + RegChars.charAt(special) + s.substring(c + 1, s.length);
          }
        }
      }
      return s;
    }

    function validarAcentos(s) {
      var AccChars = "&°–—ªº·";
      var RegChars = "  --a .";

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
        //libreria.sendMail(LMRY_script, ' [ ObtainFederalIdSubsidiaria ] ' + err);
      }
      return '';
    }

    function NameFile(a, b) {

      if (feature_Multi) {
        var nameFile = 'ART4_' + a + '_' + b + '_' + param_Subsi + '_' + param_Multi
      } else {
        var nameFile = 'ART4_' + a + '_' + b + '_' + param_Subsi;
      }

      return nameFile;
    }

    function getTaxResults(transactionID, lineUniqueKey){
      var DbolStop = false;
      var intDMinReg = 0;
      var intDMaxReg = 1000;
      var ArrReturn = [];

      var savedsearch = search.create({
        type: "customrecord_lmry_br_transaction",
        filters: [
          ["custrecord_lmry_br_transaction", "is", transactionID],
          "AND",
          ["custrecord_lmry_lineuniquekey", "equalto", lineUniqueKey],
          "AND",
          ["custrecord_lmry_br_type", "is", "ReteICA"]
        ],
        columns: [
          search.createColumn({
            name: "formulanumeric",
            formula: "{custrecord_lmry_base_amount}",
            label: "0. Base Amount"
          }),
          search.createColumn({
            name: "formulanumeric",
            formula: "{custrecord_lmry_br_total}",
            label: "1. Imposto"
          }),
          search.createColumn({
            name: "formulanumeric",
            formula: "{custrecord_lmry_br_percent}",
            label: "2. Percentage"
          }),
          search.createColumn({
            name: "formulanumeric",
            formula: "{custrecord_lmry_base_amount_local_currc}",
            label: "3. Base Amount Local Currency"
          }),
          search.createColumn({
            name: "formulanumeric",
            formula: "{custrecord_lmry_amount_local_currency}",
            label: "4. Impuesto Local Currency"
          }),
          search.createColumn({
            name: "formulatext",
            formula: "{custrecord_lmry_accounting_books}",
            label: "5. TC's"
          })
        ]
      });

      var searchresult = savedsearch.run();

      while (!DbolStop) {
        var objResult = searchresult.getRange(intDMinReg, intDMaxReg);

        if (objResult != null) {

          if (objResult.length != 1000) {
            DbolStop = true;
          }

          var intLength = objResult.length;

          for (var i = 0; i < intLength; i++) {
            var columns = objResult[i].columns;
            var arr = new Array();

            //TC
            var exchangeRate = exchange_rate(objResult[i].getValue(columns[5]));

            // 0. Base Amount
            var montoBase = objResult[i].getValue(columns[3]);
            if (montoBase != null && montoBase != 0 && montoBase != "- None -") {
              arr[0] = Number(montoBase);
            } else {
              arr[0] = objResult[i].getValue(columns[0]) * exchangeRate;

            }
            // 1. Retencion
            var impuesto = objResult[i].getValue(columns[4]);
            if (impuesto != null && impuesto != 0 && impuesto != "- None -") {
              arr[1] = Number(impuesto);
            } else {
              arr[1] = objResult[i].getValue(columns[1]) * exchangeRate;
            }

            // 2. Percent
            arr[2] = Number(objResult[i].getValue(columns[2])) * 10000;

            ArrReturn.push(arr);
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

    function getVendorData(id_vendor) {
      var vendorData = new Array();

      var vendorEntity = search.lookupFields({
        type: search.Type.VENDOR,
        id: id_vendor,
        columns: ["isperson", "companyname", "firstname", "lastname","vatregnumber", "email", "phone",
      "custentity_lmry_sunat_tipo_doc_id.custrecord_lmry_co_idtype_name"]
      });

      if (vendorEntity != null && JSON.stringify(vendorEntity) != '{}') {
        var razonSocial;
        if (vendorEntity.isperson) {
          razonSocial = vendorEntity.firstname + ' ' + vendorEntity.lastname;
        } else {
          razonSocial = vendorEntity.companyname;
        }
        razonSocial = ValidarCaracteres_Especiales(razonSocial);
        razonSocial = Valida_colombia(razonSocial);
        razonSocial = razonSocial.substring(0, 70);

        vendorData.push(razonSocial);//0

        var vatReg = vendorEntity.vatregnumber;
        if (vatReg != '' && vatReg != null && vatReg != '- None -') {
          vatReg = Valida_Codigo(vatReg);
          vatReg = vatReg.substring(0, 11);
        }else{
          vatReg = '';
        }
        vendorData.push(vatReg);//1

        var email = vendorEntity.email;
        if (email != '' && email != null && email != '- None -') {
          email = email.substring(0, 70);
        }else{
          email = '';
        }
        vendorData.push(email);//2

        var phone = vendorEntity.phone;
        if (phone != '' && phone != null && phone != '- None -') {
          phone = ValidaGuion(phone);
          phone = phone.substring(0, 10)
        }else{
          phone = '';
        }
        vendorData.push(phone);//3

        vendorData.push(vendorEntity["custentity_lmry_sunat_tipo_doc_id.custrecord_lmry_co_idtype_name"]);//4

        return vendorData;
      } else {
        log.debug('No existe vendor con este id:', id_vendor);
        return null;
      }
    }

    function getVendorAddressData(id_vendor) {
      var datos = search.create({
        type: "vendor",
        filters: [
          ["internalid", "anyof", id_vendor],
          "AND",
          ["isdefaultbilling", "is", "T"]
        ],
        columns: [
          search.createColumn({
            name: "address1",
            join: "billingAddress",
            label: "Address 1"
          }),
          search.createColumn({
            name: "address2",
            join: "billingAddress",
            label: "Address 2"
          }),
          search.createColumn({
            name: "custrecord_lmry_addr_city_id",
            join: "billingAddress",
            label: "Latam - City ID"
          }),
          search.createColumn({
            name: "custrecord_lmry_addr_prov_id",
            join: "billingAddress",
            label: "Latam - Province ID"
          })
        ]
      });

      var resultado = datos.run().getRange(0, 1000);
      //log.debug('resultado vendor', resultado);
      var arrResult = new Array();

      if (resultado.length != 0) {
        var columns = resultado[0].columns;

        var direccion = resultado[0].getValue(columns[0]) + ' ' + resultado[0].getValue(columns[1]);
        direccion = ValidarCaracteres_Especiales(direccion);
        direccion = Valida_colombia(direccion);
        direccion = direccion.substring(0, 70);
        //0. Direccion
        arrResult.push(direccion);
        //1. municipio
        arrResult.push(resultado[0].getValue(columns[2]));
        //2. departamento
        arrResult.push(resultado[0].getValue(columns[3]));
      }else{
        arrResult = ['','',''];
      }

      return arrResult;
    }

    function exchange_rate(exchangerate) {
      var auxiliar = ('' + exchangerate).split('&');
      var final = '';

      if (feature_Multi) {
        var id_libro = auxiliar[0].split('|');
        var exchange_rate = auxiliar[1].split('|');

        for (var i = 0; i < id_libro.length; i++) {
          if (Number(id_libro[i]) == Number(param_Multi)) {
            final = exchange_rate[i];
            break;
          } else {
            final = exchange_rate[0];
          }
        }
      } else {
        final = auxiliar[1];
      }
      return final;
    }

    return {
      getInputData: getInputData,
      map: map,
      reduce: reduce,
      summarize: summarize
    };

  });
