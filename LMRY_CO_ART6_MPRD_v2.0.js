/* = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =\
||   This script for customer center (Time)                     ||
||                                                              ||
||  File Name: LMRY_CO_ART6_MPRD_v2.0.js            ||
||                                                              ||
||  Version Date         Author        Remarks                  ||
||  2.0     Sep 06 2020  LatamReady    Use Script 2.0           ||
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

    var LMRY_script = "LMRY_CO_ART6_MPRD_v2.0.js";

    var objContext = runtime.getCurrentScript();

    var companyruc = '';

    //Parametros
    param_RecorID = objContext.getParameter({
      name: 'custscript_lmry_co_art6_recid'
    });
    param_Periodo = objContext.getParameter({
      name: 'custscript_lmry_co_art6_period'
    });
    param_Anual = objContext.getParameter({
      name: 'custscript_lmry_co_art6_anual'
    });
    param_Multi = objContext.getParameter({
      name: 'custscript_lmry_co_art6_mutibook'
    });
    param_FeatID = objContext.getParameter({
      name: 'custscript_lmry_co_art6_featid'
    });
    param_Subsi = objContext.getParameter({
      name: 'custscript_lmry_co_art6_subsi'
    });
    param_head = objContext.getParameter({
      name: 'custscript_lmry_co_art6_inserthead'
    });

    //************FEATURES********************
    feature_Subsi = runtime.isFeatureInEffect({
      feature: "SUBSIDIARIES"
    });
    feature_Multi = runtime.isFeatureInEffect({
      feature: "MULTIBOOK"
    });
    Feature_Lote = runtime.isFeatureInEffect({
      feature: 'LOTNUMBEREDINVENTORY'
    });
    hasJobsFeature = runtime.isFeatureInEffect({
      feature: "JOBS"
    });
    hasAdvancedJobsFeature = runtime.isFeatureInEffect({
      feature: "ADVANCEDJOBS"
    });

    var language = runtime.getCurrentScript().getParameter({
      name: 'LANGUAGE'
    }).substring(0, 2);

    function getInputData() {
      //try{
      log.debug('parametros', param_Multi + '-' + param_Subsi + '-' + param_Periodo + '-' + param_Anual);

      var whtLines = getWHTLines();
      var whtCabecera = getWHTCabecera();
      var whtJournal = getWHTJournal();

      var whtTotal = whtLines.concat(whtCabecera, whtJournal);
      log.debug('data a procesar', whtTotal);
      return whtTotal;

      // }catch(err){
      //     log.error('err', err);
      //     //libreria.sendMail(LMRY_script, ' [ getInputData ] ' + err);
      // }
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
        var ArrCustomer = new Array();
        var arrTemp = JSON.parse(context.value);
        var montoBase = 0;
        var alicuota = 0;
        var retencion = 0;

        if (arrTemp[0] == 'Journal') {
          var entityData = getCustomerData(arrTemp[3]);

          if (entityData != null) {
            var addressData = getCustAddressData(arrTemp[3]);
            addressData = addressData.split('|');

            var taxResults = getTaxResults(arrTemp[1], arrTemp[2]);

            if (taxResults.length != 0) {
              log.debug('entityData',entityData);
              montoBase = taxResults[0][0];
              alicuota = taxResults[0][2];
              retencion = taxResults[0][1];

              id_reduce = arrTemp[3] + '|' + alicuota; //ID VENDOR + ALIQUOTA

              ArrCustomer = [entityData[0], entityData[1], entityData[2], entityData[3], entityData[4], addressData[0],
                addressData[1], addressData[2]
              ];

            } else {
              //log.debug('No hay taxresult en journal');
              return false;
            }

          } else {
            return false;
          }

        } else {
          //log.debug('arrTemp[0]',arrTemp[0]);
          var entityData = getCustomerData(arrTemp[0]);
          //log.debug('entityData',entityData);
          var addressData = getCustAddressData(arrTemp[0]);
          addressData = addressData.split('|');

          var montoBase = arrTemp[1];
          var alicuota = arrTemp[2];
          var retencion = arrTemp[3];

          ArrCustomer = [entityData[0], entityData[1], entityData[2], entityData[3], entityData[4], addressData[0],
            addressData[1], addressData[2]
          ];

          id_reduce = arrTemp[0] + '|' + arrTemp[2]; //id customer + alicuota
        }

        context.write({
          key: id_reduce,
          value: {
            Customer: ArrCustomer,
            Montobase: montoBase,
            Aliquota: alicuota,
            MontoRetenido: retencion
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
      //log.error('entro en el reduce');
      var estado;
      var monto = 0;
      var ArrCustomer = new Array();
      var ArrItem = new Array();
      var monto_B = 0;
      var monto_R = 0;
      var por = '';
      var arreglo = context.values;
      //log.error('arreglo del map',arreglo);
      var tamaño = arreglo.length;
      for (var i = 0; i < tamaño; i++) {
        var obj = JSON.parse(arreglo[i]);
        /*if (obj["isError"] == "T") {
            context.write({
                key   : context.key,
                value : obj
            });
            return;
        }*/

        ArrCustomer = obj.Customer;

        monto_B += obj.Montobase;
        monto_R += obj.MontoRetenido;
        por = Number(obj.Aliquota); //*10000
        por = por.toFixed(2);
        if (por == 0) {
          por = '0.00'
        }

      }

      monto_B = redondear(monto_B);
      monto_R = redondear(monto_R);

      context.write({
        key: context.key,
        value: {
          Customer: ArrCustomer,
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
        if (param_Anual != '' && param_Anual != null) {
          var periodenddate_temp = search.lookupFields({
            type: search.Type.ACCOUNTING_PERIOD,
            id: param_Anual,
            columns: ['periodname']
          });
          //Period EndDate
          Anual = periodenddate_temp.periodname;
          Anual = Anual.substring(Anual.length - 4, Anual.length);

          log.error('nombre del año', Anual.length);
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
          log.error('valor del año', Anual.length);
        }

        context.output.iterator().each(function(key, value) {
          var obj = JSON.parse(value);
          if (obj["isError"] == "T") {
            errores.push(JSON.stringify(obj["error"]));
          } else {
            ArrCustomer = obj.Customer;
            //log.error('quiero ver como viene',ArrVendor);
            monto_base = obj.Montobase;
            MontoRet = obj.MontoRetenido;
            porc = obj.Aliquota;

            strReporte += Anual + ';' + ArrCustomer[1] + ';' + ArrCustomer[2] + ';' + ArrCustomer[0] + ';' + ArrCustomer[5] + ';' + ArrCustomer[3] + ';' + ArrCustomer[4] + ';' + ArrCustomer[6] + ';' + ArrCustomer[7] + ';' + monto_base + ';' + porc + ';' + MontoRet + '\r\n';

          }
          return true;
        });
        log.debug('strReporte', strReporte);

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
        companyruc = QuitaGuion(companyruc);

        if (strReporte == '') {
          NoData();
        } else {
          saveFile(strReporte);
        }

        //log.error('paso de guardar el archivo',idFile);
      } catch (err) {
        log.error('err', err);
        //libreria.sendMail(LMRY_script, ' [ getInputData ] ' + err);
      }
    }

    function getTaxResults(transactionID, lineUniqueKey) {
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

    function saveFile(strReporte) {
      var folderId = objContext.getParameter({
        name: 'custscript_lmry_file_cabinet_rg_co'
      });
      // Almacena en la carpeta de Archivos Generados
      if (folderId != '' && folderId != null) {
        // Extension del archivo
        if (param_head == 'T') {
          var fileExt = '.csv';
          var nameFile = NameFile() + fileExt;

          strCabecera = 'VIGENCIA' + ';' + 'TIPO DE DOCUMENTO' + ';' + 'NUMERO DE DOCUMENTO' + ';' + 'NOMBRE O RAZON SOCIAL' + ';' + 'DIRECCION DE NOTIFICACION' + ';' + 'TELEFONO' + ';' + 'EMAIL' + ';' + 'CODIGO DE MUNICIPIO' + ';' + 'CODIGO DE DEPARTAMENTO' + ';' + 'MONTO PAGO' + ';' + 'TARIFA RETENCION APLICADA' + ';' + 'MONTO RETENCION ANUAL';

          // Crea el archivo
          var reportFile = fileModulo.create({
            name: nameFile,
            fileType: fileModulo.Type.CSV,
            contents: strCabecera + '\r\n' + strReporte,
            encoding: fileModulo.Encoding.UTF8,
            folder: folderId
          });
        } else {
          var fileExt = '.txt';
          var nameFile = NameFile() + fileExt;

          // Crea el archivo
          var reportFile = fileModulo.create({
            name: nameFile,
            fileType: fileModulo.Type.PLAINTEXT,
            contents: strReporte,
            encoding: fileModulo.Encoding.UTF8,
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
          id: param_FeatID,
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
          //libreria.sendrptuserTranslate(namereport, 3, nameFile, language);
        }
      } else {
        log.debug("No se encontro folder");
      }
    }

    function redondear(number) {
      return Math.round(Number(number));
    }

    function getWHTJournal() {
      var intDMinReg = 0;
      var intDMaxReg = 1000;
      var DbolStop = false;
      var arrResult = new Array();

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

    function getWHTLines() {
      var intDMinReg = 0;
      var intDMaxReg = 1000;
      var DbolStop = false;
      //para la busqueda de transacciones
      var arrReturn = new Array();

      var savedsearch = search.load({
        /*LatamReady - CO Articulo 6 Line Level*/
        id: 'customsearch_lmry_co_art_6_line'
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
      if (hasJobsFeature && !hasAdvancedJobsFeature) {
        log.error("customermain");
        var customerColumn = search.createColumn({
          name: 'formulanumeric',
          formula: '{customermain.internalid}',
          summary: 'GROUP'
        });
        savedsearch.columns.push(customerColumn);
      } else if ((!hasJobsFeature && !hasAdvancedJobsFeature) || (!hasJobsFeature && hasAdvancedJobsFeature) || (hasJobsFeature && hasAdvancedJobsFeature)) {
        log.error("customer");
        var customerColumn = search.createColumn({
          name: "formulanumeric",
          formula: "CASE WHEN NVL({job.internalid},-1) = -1 THEN {customer.internalid} ELSE {job.customer.id} end",
          summary: "GROUP"
        });
        savedsearch.columns.push(customerColumn);
      }

      var columnaMultibook = search.createColumn({
        name: 'formulatext',
        summary: 'Group',
        formula: "{custrecord_lmry_br_transaction.custrecord_lmry_accounting_books}",
        label: " Multibook"
      });
      savedsearch.columns.push(columnaMultibook);
      var searchResult = savedsearch.run();

      while (!DbolStop) {
        var objResult = searchResult.getRange(intDMinReg, intDMaxReg);
        //log.error('tamaño de la busqueda',objResult.length);
        if (objResult != null) {
          if (objResult.length != 1000) {
            DbolStop = true;
          }

          for (var i = 0; i < objResult.length; i++) {
            var columns = objResult[i].columns;
            var arrAuxiliar = new Array();
            // 0. id Customer
            if (objResult[i].getValue(columns[3]) != '' && objResult[i].getValue(columns[3]) != null && objResult[i].getValue(columns[3]) != '- None -') {
              arrAuxiliar[0] = objResult[i].getValue(columns[3]);
            } else {
              arrAuxiliar[0] = '';
            }
            /*TC MULTIBOOK*/
            if (objResult[i].getValue(columns[4]) != null && objResult[i].getValue(columns[4]) != '- None -') {
              var exch_rate_nf = objResult[i].getValue(columns[4]);
              exch_rate_nf = exchange_rate(exch_rate_nf);
            } else {
              exch_rate_nf = 1;
            }
            //1. monto pago
            var monto_pago = objResult[i].getValue(columns[0]) * exch_rate_nf;
            arrAuxiliar[1] = Number(monto_pago.toFixed(0));
            //2. tarifa retencion aplicada
            arrAuxiliar[2] = (objResult[i].getValue(columns[1])) * 10000;
            //3. monto retencion anual
            arrAuxiliar[3] = objResult[i].getValue(columns[2]) * exch_rate_nf;

            arrReturn.push(arrAuxiliar);
          }
          if (!DbolStop) {
            intDMinReg = intDMaxReg;
            intDMaxReg += 1000;
          }
        } else {
          DbolStop = true;
        }
      }

      return arrReturn;
    }

    function getWHTCabecera() {

      var arrReturn = new Array();
      intDMinReg = 0;
      intDMaxReg = 1000;
      DbolStop = false;

      var savedsearch_2 = search.load({
        /*LatamReady - CO Articulo 6 Main Level*/
        id: 'customsearch_lmry_co_art_6_main'
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
      //   var columna_tipo_rete = search.createColumn({
      //        name: "custrecord_lmry_wht_salebase",
      //        join: "CUSTBODY_LMRY_CO_RETEICA",
      //        summary: "GROUP",
      //        label: "Sale WHT Base"
      //     });
      //       savedsearch_2.columns.push(columna_tipo_rete);

      if (hasJobsFeature && !hasAdvancedJobsFeature) {
        log.error("customermain");
        var customerColumn = search.createColumn({
          name: 'formulanumeric',
          formula: '{customermain.internalid}',
          summary: 'GROUP'
        });
        savedsearch_2.columns.push(customerColumn);
      } else if ((!hasJobsFeature && !hasAdvancedJobsFeature) || (!hasJobsFeature && hasAdvancedJobsFeature) || (hasJobsFeature && hasAdvancedJobsFeature)) {
        log.error("customer");
        var customerColumn = search.createColumn({
          name: "formulanumeric",
          formula: "CASE WHEN NVL({job.internalid},-1) = -1 THEN {customer.internalid} ELSE {job.customer.id} end",
          summary: "GROUP"
        });
        savedsearch_2.columns.push(customerColumn);
      }

      //columan4
      var columnaExchangeRate = search.createColumn({
        name: 'exchangerate',
        summary: 'Group',
        label: "Exchange Rate"
      });
      savedsearch_2.columns.push(columnaExchangeRate);


      if (feature_Multi) {
        var multibookFilter = search.createFilter({
          name: 'accountingbook',
          join: 'accountingtransaction',
          operator: search.Operator.IS,
          values: [param_Multi]
        });
        savedsearch_2.filters.push(multibookFilter);

        //columan5
        var columnaExchangeRateMulti = search.createColumn({
          name: 'exchangerate',
          summary: 'Group',
          join: "accountingTransaction",
          label: "Exchange Rate"
        });
        savedsearch_2.columns.push(columnaExchangeRateMulti);
      }

      var searchResult = savedsearch_2.run();
      //log.error('segunda busqueda',searchResult);
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
            // 0. ID customer

            if (objResult[i].getValue(columns[3]) != '' && objResult[i].getValue(columns[3]) != null && objResult[i].getValue(columns[3]) != '- None -') {
              arrAuxiliar[0] = objResult[i].getValue(columns[3]);
            } else {
              arrAuxiliar[0] = '';
            }

            //   //1.MULTIBOOK
            //   if (objResult[i].getValue(columns[4])!=null && objResult[i].getValue(columns[4])!='- None -'){
            //     var exch_rate_nf = objResult[i].getValue(columns[4]);
            //     exch_rate_nf = exchange_rate(exch_rate_nf);
            //   }else{
            //     exch_rate_nf = 1;
            //   }
            //1. monto pago
            if (feature_Multi) {
              if (objResult[i].getValue(columns[0]) != null && objResult[i].getValue(columns[0]) != '' && objResult[i].getValue(columns[0]) != '- None -' && objResult[i].getValue(columns[0]) != 'NaN') {
                arrAuxiliar[1] = Number(objResult[i].getValue(columns[0])) * Number(objResult[i].getValue(columns[5])) / Number(objResult[i].getValue(columns[4]));
              } else {
                arrAuxiliar[1] = 0.00;
              }
            } else {
              if (objResult[i].getValue(columns[0]) != null && objResult[i].getValue(columns[0]) != '' && objResult[i].getValue(columns[0]) != '- None -' && objResult[i].getValue(columns[0]) != 'NaN') {
                arrAuxiliar[1] = Number(objResult[i].getValue(columns[0]));
              } else {
                arrAuxiliar[1] = 0.00;
              }
            }


            //3. tarifa retencion aplicada
            arrAuxiliar[2] = (objResult[i].getValue(columns[1]));
            //4. monto retencion anual
            arrAuxiliar[3] = Number(objResult[i].getValue(columns[2]));


            //  //id de retencion
            //  log.error('no lo veo',objResult[i].getValue(columns[4]));
            //  if (objResult[i].getValue(columns[11])!=null && objResult[i].getValue(columns[11])!='- None -'){
            //     id_retencion = objResult[i].getValue(columns[11]);
            //  }else{
            //     id_retencion = '';
            //  }
            //  //6. monto base
            //  if (id_retencion == 1) {
            //    arrAuxiliar[5] = objResult[i].getValue(columns[6]);
            //  }else if (id_retencion == 2) {
            //    arrAuxiliar[5] = objResult[i].getValue(columns[8]);
            //  }else if (id_retencion == 3) {
            //    arrAuxiliar[5] = objResult[i].getValue(columns[7]);
            //  }


            //LLenamos los valores en el arreglo
            arrReturn.push(arrAuxiliar);
          }
          if (!DbolStop) {
            intDMinReg = intDMaxReg;
            intDMaxReg += 1000;
          }
        } else {
          DbolStop = true;
        }
      }

      return arrReturn;
    }

    function NoData() {

      var usuario = runtime.getCurrentUser();

      var periodenddate_temp = search.lookupFields({
        type: search.Type.ACCOUNTING_PERIOD,
        id: param_Periodo,
        columns: ['periodname']
      });

      //Period StartDate
      var periodname = periodenddate_temp.periodname;

      var employee = search.lookupFields({
        type: search.Type.EMPLOYEE,
        id: usuario.id,
        columns: ['firstname', 'lastname']
      });
      var usuarioName = employee.firstname + ' ' + employee.lastname;

      var report = search.lookupFields({
        type: 'customrecord_lmry_co_features',
        id: param_FeatID,
        columns: ['name']
      });
      namereport = report.name;

      var generatorLog = recordModulo.load({
        type: 'customrecord_lmry_co_rpt_generator_log',
        id: param_RecorID
      });

      //Nombre de Archivo
      generatorLog.setValue({
        fieldId: 'custrecord_lmry_co_rg_name',
        value: 'No existe informacion para los criterios seleccionados.'
      });
      //Creado Por
      generatorLog.setValue({
        fieldId: 'custrecord_lmry_co_rg_employee',
        value: usuarioName
      });
      generatorLog.setValue({
        fieldId: 'custrecord_lmry_co_rg_transaction',
        value: namereport
      });

      var recordId = generatorLog.save();
    }


    function Remplaza_tildes(s) {
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

    function QuitaGuion(s) {
      var AccChars = "-./(),;_";
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

    function Valida_caracteres_blanco(s) {
      var AccChars = "!“#$%&/()=\\-+/*ªº.,;ªº-_[]";
      var RegChars = "                           ";
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

    function completar_cero(long, valor) {
      var length = ('' + valor).length;
      if (length <= long) {
        if (long != length) {
          for (var i = length; i < long; i++) {
            valor = '0' + valor;
          }
        } else {
          return valor;
        }
        return valor;
      } else {
        valor = ('' + valor).substring(0, long);
        return valor;
      }
    }

    function completar_espacio(long, valor) {
      if ((('' + valor).length) <= long) {
        if (long != ('' + valor).length) {
          for (var i = (('' + valor).length); i < long; i++) {
            valor = ' ' + valor;
          }
        } else {
          return valor;
        }
        return valor;
      } else {
        valor = valor.substring(0, long);
        return valor;
      }
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

    function NameFile() {
      var nameFile = 'ART6_';
      if (param_Anual != '' && param_Anual != null) {
        var periodenddate_temp = search.lookupFields({
          type: search.Type.ACCOUNTING_PERIOD,
          id: param_Anual,
          columns: ['periodname']
        });
        //Period EndDate
        Anual = periodenddate_temp.periodname;
        Anual = Anual.substring(Anual.length - 5, Anual.length);
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


      var AAAA = Anual;

      if (feature_Multi || feature_Multi == 'T') {
        // if (paramContador != 0) {
        nameFile += companyruc + '_' + AAAA + '_' + param_Subsi + '_' + param_Multi;
        // } else {
        //     nameFile += companyruc +'_'+AAAA + '_' + param_Subsi + '_' + param_Multi;
        // }
      } else {
        //if (paramContador != 0) {
        nameFile += companyruc + '_' + AAAA + '_' + param_Subsi;
        // } else {
        //     nameFile += companyruc +'_'+AAAA + '_' + param_Subsi;
        // }
      }

      return nameFile;
    }

    function getCustomerData(idCustomer) {

      var customerEntity = search.lookupFields({
        type: search.Type.CUSTOMER,
        id: idCustomer,
        columns: ['isperson', 'companyname', 'firstname', 'lastname', 'vatregnumber', 'phone', 'email', "custentity_lmry_sunat_tipo_doc_id.custrecord_lmry_co_idtype_name"]
      });

      //log.debug('customerEntity',customerEntity);

      if (customerEntity != null && JSON.stringify(customerEntity) != '{}') {
        //1.tipo de documento
        var ide = customerEntity["custentity_lmry_sunat_tipo_doc_id.custrecord_lmry_co_idtype_name"];
        if (ide == 'CC' || ide == 'CE' || ide == 'TI' || ide == 'NIT' || ide == 'PA') {
          ide = completar_espacio(3, ide);
        } else {
          ide = '';
        }
        //2
        var vatregnumber = customerEntity.vatregnumber;
        if (vatregnumber == '' || vatregnumber == null || vatregnumber == 'NaN') {
          vatregnumber = '';
        }
        var campo2 = QuitaGuion(vatregnumber).substring(0, 11);
        //log.error('campo2',campo2);
        var campo3 = '';
        if (customerEntity.isperson) {
          var first = customerEntity.firstname;
          if (first == '' || first == null || first == 'NaN') {
            first = '';
          }

          var last = customerEntity.lastname;
          if (last == '' || last == null || last == 'NaN') {
            last = '';
          }

          campo3 = first + ' ' + last;
        } else {
          var raz = customerEntity.companyname;
          if (raz == '' || raz == null || raz == 'NaN') {
            raz = '';
          }

          campo3 = raz;
        }
        campo3 = Remplaza_tildes(campo3);
        campo3 = Valida_caracteres_blanco(campo3);
        campo3 = campo3.substring(0, 70);

        //5. telefono
        var campo5 = customerEntity.phone;
        if (campo5 != '' && campo5 != null && campo5 != 'NaN') {
          campo5 = QuitaGuion(customerEntity.phone);
          campo5 = campo5.substring(0, 10);
        } else {
          campo5 = '';
        }
        //6. email
        var campo6 = customerEntity.email;
        if (campo6 == '' || campo6 == null || campo6 == 'NaN') {
          campo6 = '';
        }

        var arrData = [campo3, ide, campo2, campo5, campo6];

        return arrData;
      } else {
        return null;
      }

    }

    function getCustAddressData(id_customer) {
      var datos = search.create({
        type: "customer",
        filters: [
          ["internalid", "anyof", id_customer],
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
      //log.error('resultado del customer direccion',resultado);
      if (resultado.length != 0) {
        var columns = resultado[0].columns;

        direccion = resultado[0].getValue(columns[0]) + ' ' + resultado[0].getValue(columns[1]);

        direccion = Valida_caracteres_blanco(direccion);
        direccion = Remplaza_tildes(direccion);
        direccion = direccion.substring(0, 70);
        municipio = resultado[0].getValue(columns[2]);
        departamento = resultado[0].getValue(columns[3]);
      } else {
        direccion = '';
        municipio = '';
        departamento = '';
      }

      return direccion + '|' + municipio + '|' + departamento;
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
