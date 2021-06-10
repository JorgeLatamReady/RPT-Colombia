/* = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =\
||   This script for customer center (Time)                     ||
||                                                              ||
||  File Name: LMRY_CO_BalCompTerceros_MPRDC_v2.0.js            ||
||                                                              ||
||  Version Date         Author        Remarks                  ||
||  2.0     Jun 18 2018  LatamReady    Use Script 2.0           ||
\= = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = */
/**
 * @NApiVersion 2.x
 * @NScriptType MapReduceScript
 * @NModuleScope Public
 */
define(['N/search', 'N/log', 'require', 'N/file', 'N/runtime', 'N/query', "N/format", "N/record", "N/task", "N/config", "./CO_Library_Mensual/LMRY_CO_Reportes_LBRY_V2.0.js"],

  function(search, log, require, fileModulo, runtime, query, format, recordModulo, task, config, libreria) {

    /**
     * Input Data for processing
     *
     * @return Array,Object,Search,File
     *
     * @since 2016.1
     */

    var objContext = runtime.getCurrentScript();
    var LMRY_script = "LMRY_CO_InvBalance_MPRD_v2.0.js";

    var paramMultibook = objContext.getParameter({
      name: 'custscript_test_invbal_multibook'
    });
    var paramRecordID = objContext.getParameter({
      name: 'custscript_test_invbal_logid'
    });
    var paramSubsidy = objContext.getParameter({
      name: 'custscript_test_invbal_subsi'
    });
    var paramPeriod = objContext.getParameter({
      name: 'custscript_test_invbal_periodo'
    });
    var paramPUC = objContext.getParameter({
      name: 'custscript_test_invbal_lastpuc'
    });
    var paramFileID = objContext.getParameter({
      name: 'custscript_test_invbal_fileid'
    });

    var ArrData = new Array();

    var periodYearIni;
    var periodMonthIni;
    var periodIsAdjust = false;
    var periodStartDate;

    var featuresubs = runtime.isFeatureInEffect({
      feature: "SUBSIDIARIES"
    });
    var feamultibook = runtime.isFeatureInEffect({
      feature: "MULTIBOOK"
    });
    var featurejobs = runtime.isFeatureInEffect({
      feature: "JOBS"
    });

    var entityCustomer = false;
    var entityVendor = false;
    var entityEmployee = false;
    var entityOtherName = false;

    var entity_name;
    var entity_id;
    var entity_nit;

    function getInputData() {
      try {
        log.debug('getInputData', 'getInputData');
        log.debug('parametros:', 'Multibook -' + paramMultibook + ' logID -' + paramRecordID + ' Subsi -' + paramSubsidy + ' periodo -' + paramPeriod + ' PUC -' + paramPUC + ' FILE ID -' + paramFileID);
        ParametrosYFeatures();
        // Obtiene años ya procesados
        var ArrProcessedYears = ObtenerAñosProcesados();
        // Obtiene los periodos Fiscal Year (desde el inicio hasta un año antes del periodo de generación)
        var ArrYears = ObtenerAñosFiscales();

        OrdenarAños(ArrYears);
        OrdenarAños(ArrProcessedYears);

        for (var i = 0; i < ArrYears.length; i++) {
          var flag = false;

          for (var j = 0; j < ArrProcessedYears.length; j++) {
            if (ArrProcessedYears[j][1] == ArrYears[i][1] && ArrProcessedYears[j][4] == paramPUC) {
              flag = true;
              break;
            }
          }

          if (!flag) {
            var arrTemporal = new Array();
            arrTemporal = ObtenerData(ArrYears[i][0], false); //si filtra por parametro PUC
            var arrTemporalSpecific = new Array(); //obtiene specific transactions

            if (feamultibook) {
              arrTemporalSpecific = ObtenerData(ArrYears[i][0], true); //no filtra por parametro puc
              Array.prototype.push.apply(arrTemporal, arrTemporalSpecific);
            }

            if (arrTemporal.length != 0) {
              arrTemporal = AgruparPorCuenta(arrTemporal); //agrupa por cuenta y entity

              for (var x = 0; x < arrTemporal.length; x++) {
                arrTemporal[x][4] = ArrYears[i][1];
                if (featuresubs) {
                  arrTemporal[x][5] = paramSubsidy;
                }
                ArrData.push(arrTemporal[x]);
              }
            }

            //actualizarThirdProc(ArrYears[i][1]);
          }
        }
        log.debug('ArrData', ArrData);
        return ArrData;

      } catch (err) {
        log.error('err', err);
        libreria.sendMail(LMRY_script, ' [ getInputData ] ' + err);
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

        if (paramPUC == '' || paramPUC == null) {
          paramPUC = 1;
        }

        var arrTemp = JSON.parse(context.value);

        var account_lookup = search.lookupFields({
          type: search.Type.ACCOUNT,
          id: Number(arrTemp[0]),
          columns: ['custrecord_lmry_co_puc_d6_id']
        });

        var firtsDigitPUC = ((account_lookup.custrecord_lmry_co_puc_d6_id)[0].text).charAt(0);

        if (firtsDigitPUC == paramPUC) {
          //actualizarThirdData(arrTemp, firtsDigitPUC);
        }
      } catch (err) {
        //log.error('err', err);
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
    function summarize(context) {
      try {
        log.debug('summarize', 'summarize');
        ParametrosYFeatures();
        // Obtiene los periodos Fiscal Year (desde el inicio hasta un año antes del periodo de generación)
        ArrYears = ObtenerAñosFiscales();
        OrdenarAños(ArrYears);
        log.debug('ArrYears en summarize', ArrYears);
        var arrSaldoAnterior = obtenerSaldoAnterior(ArrYears[ArrYears.length - 1][1]); //saldos del inicio de los tiempos hasta un año antes al periodo de generación.
        log.debug('arrSaldoAnterior',arrSaldoAnterior);

        if (paramFileID == null || paramFileID == '') {
          var idfile = savefile(ConvertirAString(arrSaldoAnterior));
        }else{
          var file = fileModulo.load({
            id: paramFileID
          });
          var lineas = file.getContents();
          var idfile = savefile(lineas + ConvertirAString(arrSaldoAnterior));
          log.debug('idfile',idfile);
        }
        // Obtener todos los periodos
        var ArrAllPeriods = ObtenerPeriodos();
        // Obtener periodos del año
        var ArrYearPeriods = ObtenerPeriodosDelAño(ArrAllPeriods);
        log.debug('Periodos faltantes a procesar, para puc '+paramPUC+':',ArrYearPeriods);
        if (ArrYearPeriods.length != 0) {
          ArrYearPeriods = ArrYearPeriods.map(function rem(e) {return e[0]});
          ArrYearPeriods = ArrYearPeriods.join(',');
          log.debug('ArrYearPeriods',ArrYearPeriods);
        }else{
          ArrYearPeriods = '';
        }
        llamarSchedule(idfile,ArrYearPeriods);

      } catch (err) {
        log.error('err', err);
        libreria.sendMail(LMRY_script, ' [ getInputData ] ' + err);
      }
    }

    function ObtenerPeriodosDelAño(ArrAllPeriods) {
      var ArrReturn = new Array();

      for (var i = 0; i < ArrAllPeriods.length; i++) {
        var tempYear = format.parse({
          value: ArrAllPeriods[i][1],
          type: format.Type.DATE
        }).getFullYear();

        var tempMonth = format.parse({
          value: ArrAllPeriods[i][1],
          type: format.Type.DATE
        }).getMonth();

        if (periodIsAdjust) {
          if (tempYear == periodYearIni && tempMonth <= periodMonthIni && paramPeriod != ArrAllPeriods[i][0]) {
            var arr = new Array();
            arr[0] = ArrAllPeriods[i][0];
            arr[1] = ArrAllPeriods[i][1];
            ArrReturn.push(arr);
          }
        } else {
          if (tempYear == periodYearIni && tempMonth < periodMonthIni) {
            var arr = new Array();
            arr[0] = ArrAllPeriods[i][0];
            arr[1] = ArrAllPeriods[i][1];
            ArrReturn.push(arr);
          }
        }
      }

      ArrReturn = OrdenarPeriodosPorMes(ArrReturn);
      return ArrReturn;
    }

    function obtenerSaldoAnterior(lastYear) {
      var intDMinReg = 0;
      var intDMaxReg = 1000;
      var DbolStop = false;
      var ArrReturn = new Array();

      var savedsearch = search.load({
        /*LatamReady - CO Inventory and Balance Test*/
        id: 'customsearch_test_co_inv_bal'
      });

      var pucFilter = search.createFilter({
        name: 'custrecord_lmry_co_terceros_puc6',
        operator: search.Operator.STARTSWITH,
        values: [paramPUC]
      });
      savedsearch.filters.push(pucFilter);

      var periodFilter = search.createFilter({
        name: 'custrecord_lmry_co_terceros_year',
        operator: search.Operator.LESSTHANOREQUALTO,
        values: [lastYear]
      })
      savedsearch.filters.push(periodFilter);

      if (featuresubs) {
        var subsidiaryFilter = search.createFilter({
          name: 'custrecord_lmry_co_terceros_subsi',
          operator: search.Operator.IS,
          values: [paramSubsidy]
        });
        savedsearch.filters.push(subsidiaryFilter);
      }

      if (feamultibook) {
        var multibookFilter = search.createFilter({
          name: 'custrecord_lmry_co_terceros_multibook',
          operator: search.Operator.IS,
          values: [paramMultibook]
        });
        savedsearch.filters.push(multibookFilter);
      }

      var savedsearchResult = savedsearch.run();

      while (!DbolStop) {
        var objResult = savedsearchResult.getRange(intDMinReg, intDMaxReg);

        if (objResult != null) {

          if (objResult.length != 1000) {
            DbolStop = true;
          }

          for (var i = 0; i < objResult.length; i++) {
            var columns = objResult[i].columns;
            var arrAuxiliar = new Array();
            // 0. PUC 4 digitos
            // 1. Debit
            // 2. Credit
            // 3. Balance
            for (var j = 0; j < columns.length; j++) {
              if (objResult[i].getValue(columns[j]) != null && objResult[i].getValue(columns[j]) != '- None -' && objResult[i].getValue(columns[j]) != 'NaN' && objResult[i].getValue(columns[j]) != 'undefined') {
                arrAuxiliar[j] = objResult[i].getValue(columns[j]);
              } else {
                arrAuxiliar[j] = '';
              }
            }
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

    function actualizarThirdProc(anioProcesado) {

      var record = recordModulo.create({
        type: 'customrecord_lmry_co_terceros_procesados',
      });

      if (featuresubs || featuresubs == 'T') {
        record.setValue({
          fieldId: 'custrecord_lmry_co_subsi_procesado',
          value: '' + paramSubsidy
        });
      } else {

        var configpage = config.load({
          type: config.Type.COMPANY_INFORMATION
        });
        var idProce = configpage.getValue('id');

        record.setValue({
          fieldId: 'custrecord_lmry_co_subsi_procesado',
          value: '' + idProce
        });
      }

      record.setValue({
        fieldId: 'custrecord_lmry_co_year_procesado',
        value: anioProcesado
      });

      record.setValue({
        fieldId: 'custrecord_lmry_co_puc_procesado',
        value: '' + paramPUC
      });

      if (feamultibook || feamultibook == 'T') {
        record.setValue({
          fieldId: 'custrecord_lmry_co_multibook_procesado',
          value: '' + paramMultibook
        });
      }

      record.save();
    }

    function actualizarThirdData(arrTemp, puc) {

      var record = recordModulo.create({
        type: 'customrecord_lmry_co_terceros_data',
      });
      // 7. PUC 6
      record.setValue({
        fieldId: 'custrecord_lmry_co_terceros_puc6',
        value: '' + puc
      });
      // 0. Account
      record.setValue({
        fieldId: 'custrecord_lmry_co_terceros_account',
        value: arrTemp[0]
      });
      // 1. Debit
      var debit = 0;
      if (arrTemp[1] != null && arrTemp[1] != '') {
        debit = arrTemp[1];
      }
      record.setValue({
        fieldId: 'custrecord_lmry_co_terceros_debit',
        value: debit
      });
      // 2. Credit
      var credit = 0;
      if (arrTemp[2] != null && arrTemp[2] != '') {
        credit = arrTemp[2];
      }
      record.setValue({
        fieldId: 'custrecord_lmry_co_terceros_credit',
        value: credit
      });
      // 3. Entity
      var json_entity = {};
      var flag_entity = ObtenerEntidad(arrTemp[3]);

      if (flag_entity) {
        json_entity.name = entity_name;
        json_entity.nit = entity_nit;
        json_entity.internalid = arrTemp[3];

        record.setValue({
          fieldId: 'custrecord_lmry_co_terceros_entity',
          value: JSON.stringify(json_entity)
        });
      }
      // 4. Year
      record.setValue({
        fieldId: 'custrecord_lmry_co_terceros_year',
        value: arrTemp[4]
      });
      // 5. Multibook
      if (feamultibook || feamultibook == 'T') {
        record.setValue({
          fieldId: 'custrecord_lmry_co_terceros_multibook',
          value: '' + paramMultibook
        });
      }
      // 6. Subsidiary
      if (featuresubs || featuresubs == 'T') {
        record.setValue({
          fieldId: 'custrecord_lmry_co_terceros_subsi',
          value: '' + paramSubsidy
        });
      }

      var id = record.save();
    }

    function AgruparPorCuenta(ArrTemp) {
      var ArrReturn = new Array();

      ArrReturn.push(ArrTemp[0]);

      for (var i = 1; i < ArrTemp.length; i++) {
        var intLength = ArrReturn.length;
        for (var j = 0; j < intLength; j++) {
          //Agrupa por cuenta y por entity
          if (ArrTemp[i][0] == ArrReturn[j][0] && ArrTemp[i][3] == ArrReturn[j][3]) {
            ArrReturn[j][1] = Math.abs(ArrReturn[j][1]) + Math.abs(ArrTemp[i][1]);
            ArrReturn[j][2] = Math.abs(ArrReturn[j][2]) + Math.abs(ArrTemp[i][2]);
            break;
          }
          if (j == ArrReturn.length - 1) {
            ArrReturn.push(ArrTemp[i]);
          }
        }
      }

      return ArrReturn;
    }

    function ObtenerEntidad(paramEntity) {
      try {
        if (paramEntity != null && paramEntity != '') {

          var entity_customer_temp = search.lookupFields({
            type: search.Type.CUSTOMER,
            id: Number(paramEntity),
            columns: ['entityid', 'firstname', 'lastname', 'companyname', 'internalid', 'vatregnumber']
          });

          var entity_id;

          entity_nit = entity_customer_temp.vatregnumber;

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
              columns: ['entityid', 'firstname', 'lastname', 'companyname', 'internalid', 'vatregnumber']
            });

            entity_nit = entity_vendor_temp.vatregnumber;

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
                columns: ['entityid', 'firstname', 'lastname', 'internalid', 'custentity_lmry_sv_taxpayer_number']
              });

              entity_nit = entity_employee_temp.custentity_lmry_sv_taxpayer_number;

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
      } catch (err) {
        log.error('err', err);
        log.error('paramEntity', paramEntity);
        return false;
      }
    }

    function ConvertirAString(arrData) {
      var str_return = '';
      for (var i = 0; i < arrData.length; i++) {
        str_return += arrData[i].join('|')+'\r\n';
      }
      log.debug('str_return',str_return);
      return str_return;
    }

    function llamarSchedule(idfile , periodMov) {
      var params = {};
      params['custscript_test_co_invbalv2_logid'] = paramRecordID;
      params['custscript_test_co_invbalv2_periodo'] = paramPeriod;
      params['custscript_test_co_invbalv2_fileid'] = idfile;
      params['custscript_test_co_invbalv2_puc'] = paramPUC;
      params['custscript_test_co_invbalv2_period_res'] = periodMov;

      if (featuresubs) {
        params['custscript_test_co_invbalv2_subsi'] = paramSubsidy;
      }
      if (feamultibook) {
        params['custscript_test_co_invbalv2_multibook'] = paramMultibook
      }

      var RedirecSchdl = task.create({
        taskType: task.TaskType.SCHEDULED_SCRIPT,
        scriptId: 'customscript_test_co_inv_bal_v2_schdl',
        deploymentId: 'customdeploy_test_co_inv_bal_v2_schdl',
        params: params
      });
      log.debug('llamando a schedule');
      RedirecSchdl.submit();
    }

    function savefile(Final_string) {
      var FolderId = objContext.getParameter({
        name: 'custscript_lmry_file_cabinet_rg_co'
      });

      // Almacena en la carpeta de Archivos Generados
      if (FolderId != '' && FolderId != null) {
        var Final_NameFile = 'INVENTARIO_BALANCE_TEMPORAL' + '.txt';
        // Crea el archivo.xls
        var file = fileModulo.create({
          name: Final_NameFile,
          fileType: fileModulo.Type.PLAINTEXT,
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

        return idfile;
      }
    }

    function OrdenarPeriodosPorMes(arrTemporal) {
      var swapped;

      do {
        swapped = false;
        for (var i = 0; i < arrTemporal.length - 1; i++) {
          var a = format.parse({
            value: arrTemporal[i][1],
            type: format.Type.DATE
          }).getMonth();

          var b = format.parse({
            value: arrTemporal[i + 1][1],
            type: format.Type.DATE
          }).getMonth();

          if (Number(a) > Number(b)) {
            var temp = new Array();
            temp = arrTemporal[i];
            arrTemporal[i] = arrTemporal[i + 1];
            arrTemporal[i + 1] = temp;
            swapped = true;
          }
        }

      } while (swapped);

      return arrTemporal;
    }

    function ObtenerPeriodos() {
      var intDMinReg = 0;
      var intDMaxReg = 1000;

      var DbolStop = false;
      var ArrReturn = new Array();
      var cont = 0;

      var busqueda = search.create({
        type: search.Type.ACCOUNTING_PERIOD,
        filters: [
          search.createFilter({
            name: 'isquarter',
            operator: search.Operator.IS,
            values: ['F']
          }),
          search.createFilter({
            name: 'isinactive',
            operator: search.Operator.IS,
            values: ['F']
          }),
          search.createFilter({
            name: 'isyear',
            operator: search.Operator.IS,
            values: ['F']
          })
        ],
        columns: ['internalid', 'startdate']
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
            // 0. Internal ID
            arrAuxiliar[0] = objResult[i].getValue(columns[0]);
            // 1. Start Date
            if (objResult[i].getValue(columns[1]) != null && objResult[i].getValue(columns[1]) != '- None -' && objResult[i].getValue(columns[1]) != 'NaN' && objResult[i].getValue(columns[1]) != 'undefined') {
              arrAuxiliar[1] = objResult[i].getValue(columns[1]);
            } else {
              arrAuxiliar[1] = '';
            }

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

    function OrdenarAños(arrTemporal) {
      var swapped;

      do {
        swapped = false;
        for (var i = 0; i < arrTemporal.length - 1; i++) {
          if (arrTemporal[i][1] > arrTemporal[i + 1][1]) {
            var temp = new Array();
            temp = arrTemporal[i];
            arrTemporal[i] = arrTemporal[i + 1];
            arrTemporal[i + 1] = temp;
            swapped = true;
          }
        }

      } while (swapped);

      return arrTemporal;
    }

    function ObtenerAñosProcesados() {
      var intDMinReg = 0;
      var intDMaxReg = 1000;
      var DbolStop = false;

      var ArrReturn = new Array();
      var cont = 0;

      var busqueda = search.create({
        type: 'customrecord_lmry_co_terceros_procesados',
        filters: [
          ['isinactive', 'is', 'F']
        ],
        columns: ['internalid', 'custrecord_lmry_co_year_procesado', 'custrecord_lmry_co_multibook_procesado', 'custrecord_lmry_co_subsi_procesado', 'custrecord_lmry_co_puc_procesado']
      });

      if (feamultibook) {
        var multibookFilter = search.createFilter({
          name: 'custrecord_lmry_co_multibook_procesado',
          operator: search.Operator.IS,
          values: [paramMultibook]
        });
        busqueda.filters.push(multibookFilter);
      }

      if (featuresubs) {
        var subsidiaryFilter = search.createFilter({
          name: 'custrecord_lmry_co_subsi_procesado',
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
            // 0. Internal ID
            if (objResult[i].getValue(columns[0]) != null && objResult[i].getValue(columns[0]) != '- None -' && objResult[i].getValue(columns[0]) != 'NaN' && objResult[i].getValue(columns[0]) != 'undefined') {
              arrAuxiliar[0] = objResult[i].getValue(columns[0]);
            } else {
              arrAuxiliar[0] = '';
            }
            // 1. Año
            if (objResult[i].getValue(columns[1]) != null && objResult[i].getValue(columns[1]) != '- None -' && objResult[i].getValue(columns[1]) != 'NaN' && objResult[i].getValue(columns[1]) != 'undefined') {
              arrAuxiliar[1] = objResult[i].getValue(columns[1]);
            } else {
              arrAuxiliar[1] = '';
            }
            // 2. Multibook
            if (objResult[i].getValue(columns[2]) != null && objResult[i].getValue(columns[2]) != '- None -' && objResult[i].getValue(columns[2]) != 'NaN' && objResult[i].getValue(columns[2]) != 'undefined') {
              arrAuxiliar[2] = objResult[i].getValue(columns[2]);
            } else {
              arrAuxiliar[2] = '';
            }
            // 3. Subsidiaria
            if (objResult[i].getValue(columns[3]) != null && objResult[i].getValue(columns[3]) != '- None -' && objResult[i].getValue(columns[3]) != 'NaN' && objResult[i].getValue(columns[3]) != 'undefined') {
              arrAuxiliar[3] = objResult[i].getValue(columns[3]);
            } else {
              arrAuxiliar[3] = '';
            }
            // 4. PUC
            if (objResult[i].getValue(columns[4]) != null && objResult[i].getValue(columns[4]) != '- None -' && objResult[i].getValue(columns[4]) != 'NaN' && objResult[i].getValue(columns[4]) != 'undefined') {
              arrAuxiliar[4] = objResult[i].getValue(columns[4]);
            } else {
              arrAuxiliar[4] = '';
            }

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

    function ObtenerData(periodYearIniID, isSpecific) {
      var intDMinReg = 0;
      var intDMaxReg = 1000;
      var DbolStop = false;
      var ArrReturn = new Array();
      var cont = 0;

      var savedsearch = search.load({
        /*LatamReady - CO Balance Comp Terceros Data*/
        id: 'customsearch_lmry_co_bal_comp_terc_data'
      });

      if (featuresubs) {
        var subsidiaryFilter = search.createFilter({
          name: 'subsidiary',
          operator: search.Operator.IS,
          values: [paramSubsidy]
        });
        savedsearch.filters.push(subsidiaryFilter);
      }
      // Movimientos por año
      var periodFilter = search.createFilter({
        name: 'postingperiod',
        operator: search.Operator.IS,
        values: [periodYearIniID]
      });
      savedsearch.filters.push(periodFilter);

      if (feamultibook) {
        var amountFilter = search.createFilter({
          name: 'formulanumeric',
          operator: search.Operator.EQUALTO,
          formula: "CASE WHEN NVL({accountingtransaction.debitamount},0) - NVL({accountingtransaction.creditamount},0) <> 0 THEN 1 ELSE 0 END",
          values: [1]
        });
        savedsearch.filters.push(amountFilter);

        if (isSpecific) {
          var specificFilter = search.createFilter({
            name: 'bookspecifictransaction',
            operator: search.Operator.IS,
            values: ['T']
          });

          savedsearch.filters.push(specificFilter);
        } else {
          var specificFilter = search.createFilter({
            name: 'bookspecifictransaction',
            operator: search.Operator.IS,
            values: ['F']
          });

          savedsearch.filters.push(specificFilter);
          /* !!! ESTE FILTRO DEBERIA SER AFUERA DEL IF, REVISAR LUEGO SI EN VERDAD AFECTA.
          EN EL MAP SE HACE UN FILTRADO POR EL PARAMPUC IGUAL*/
          var pucFilter = search.createFilter({
            name: 'formulatext',
            formula: '{account.custrecord_lmry_co_puc_d6_id}',
            operator: search.Operator.STARTSWITH,
            values: [paramPUC]
          });
          savedsearch.filters.push(pucFilter);
        }

        var multibookFilter = search.createFilter({
          name: 'accountingbook',
          join: 'accountingtransaction',
          operator: search.Operator.IS,
          values: [paramMultibook]
        });
        savedsearch.filters.push(multibookFilter);

        //columan4
        var columnaDebit = search.createColumn({
          name: 'formulacurrency',
          formula: "{accountingtransaction.debitamount}",
          summary: 'SUM'
        });
        savedsearch.columns.push(columnaDebit);
        //columna5
        var columnaCredit = search.createColumn({
          name: 'formulacurrency',
          formula: "{accountingtransaction.creditamount}",
          summary: 'SUM'
        });
        savedsearch.columns.push(columnaCredit);
        //columna6
        var columnaActMulti = search.createColumn({
          name: 'account',
          join: 'accountingtransaction',
          summary: 'GROUP',
          sort: search.Sort.ASC
        });
        savedsearch.columns.push(columnaActMulti);

      } else {

        var amountFilter = search.createFilter({
          name: 'formulanumeric',
          operator: search.Operator.EQUALTO,
          formula: "CASE WHEN NVL({debitamount},0) - NVL({creditamount},0) <> 0 THEN 1 ELSE 0 END",
          values: [1]
        });
        savedsearch.filters.push(amountFilter);

      }

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

            if (feamultibook || feamultibook == 'T') {
              // 0. Account
              if (objResult[i].getValue(columns[6]) != null && objResult[i].getValue(columns[6]) != '- None -' && objResult[i].getValue(columns[6]) != 'NaN' && objResult[i].getValue(columns[6]) != 'undefined') {
                arr[0] = objResult[i].getValue(columns[6]);
              } else {
                arr[0] = '';
              }
              // 1. Debit
              if (objResult[i].getValue(columns[4]) != null && objResult[i].getValue(columns[4]) != '- None -' && objResult[i].getValue(columns[4]) != 'NaN' && objResult[i].getValue(columns[4]) != 'undefined') {
                arr[1] = objResult[i].getValue(columns[4]);
              } else {
                arr[1] = '';
              }
              // 2. Credit
              if (objResult[i].getValue(columns[5]) != null && objResult[i].getValue(columns[5]) != '- None -' && objResult[i].getValue(columns[5]) != 'NaN' && objResult[i].getValue(columns[5]) != 'undefined') {
                arr[2] = objResult[i].getValue(columns[5]);
              } else {
                arr[2] = '';
              }

            } else {
              // 0. Account
              if (objResult[i].getValue(columns[0]) != null && objResult[i].getValue(columns[0]) != '- None -' && objResult[i].getValue(columns[0]) != 'NaN' && objResult[i].getValue(columns[0]) != 'undefined') {
                arr[0] = objResult[i].getValue(columns[0]);
              } else {
                arr[0] = '';
              }
              // 1. Debit
              if (objResult[i].getValue(columns[1]) != null && objResult[i].getValue(columns[1]) != '- None -' && objResult[i].getValue(columns[1]) != 'NaN' && objResult[i].getValue(columns[1]) != 'undefined') {
                arr[1] = objResult[i].getValue(columns[1]);
              } else {
                arr[1] = '';
              }
              // 2. Credit
              if (objResult[i].getValue(columns[2]) != null && objResult[i].getValue(columns[2]) != '- None -' && objResult[i].getValue(columns[2]) != 'NaN' && objResult[i].getValue(columns[2]) != 'undefined') {
                arr[2] = objResult[i].getValue(columns[2]);
              } else {
                arr[2] = '';
              }

            }
            // 3. Entity
            if (objResult[i].getValue(columns[3]) != null && objResult[i].getValue(columns[3]) != '' && objResult[i].getValue(columns[3]) != '- None -') {
              arr[3] = objResult[i].getValue(columns[3]);
            } else {
              arr[3] = '';
            }

            ArrReturn[cont] = arr;
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

    function ParametrosYFeatures() {

      if (paramPUC == null) {
        paramPUC = 1;
      }
      log.debug('parametros:', 'Multibook -' + paramMultibook + ' logID -' + paramRecordID + ' Subsi -' + paramSubsidy + ' periodo -' + paramPeriod + ' PUC -' + paramPUC);

      var period_temp = search.lookupFields({
        type: search.Type.ACCOUNTING_PERIOD,
        id: paramPeriod,
        columns: ['startdate', 'isadjust', 'enddate']
      });

      periodStartDate = period_temp.startdate;
      periodIsAdjust = period_temp.isadjust;

      periodYearIni = format.parse({
        value: periodStartDate,
        type: format.Type.DATE
      }).getFullYear();

      periodMonthIni = format.parse({
        value: periodStartDate,
        type: format.Type.DATE
      }).getMonth();

    }

    function ObtenerAñosFiscales() {
      var intDMinReg = 0;
      var intDMaxReg = 1000;
      var DbolStop = false;

      var ArrReturn = new Array();
      var cont = 0;

      var busqueda = search.create({
        type: search.Type.ACCOUNTING_PERIOD,
        filters: [
          search.createFilter({
            name: 'isyear',
            operator: search.Operator.IS,
            values: ['T']
          }),
          search.createFilter({
            name: 'isinactive',
            operator: search.Operator.IS,
            values: ['F']
          })
        ],
        columns: ['internalid', 'startdate']
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
            // 0. Internal ID
            if (objResult[i].getValue(columns[0]) != null && objResult[i].getValue(columns[0]) != '- None -' && objResult[i].getValue(columns[0]) != 'NaN' && objResult[i].getValue(columns[0]) != 'undefined') {
              arrAuxiliar[0] = objResult[i].getValue(columns[0]);
            } else {
              arrAuxiliar[0] = '';
            }
            // 1. Start Date
            if (objResult[i].getValue(columns[1]) != null && objResult[i].getValue(columns[1]) != '- None -' && objResult[i].getValue(columns[1]) != 'NaN' && objResult[i].getValue(columns[1]) != 'undefined') {
              arrAuxiliar[1] = objResult[i].getValue(columns[1]);
            } else {
              arrAuxiliar[1] = '';
            }

            var startDateYearTemp = format.parse({
              value: arrAuxiliar[1],
              type: format.Type.DATE
            }).getFullYear();

            arrAuxiliar[1] = startDateYearTemp;

            if (startDateYearTemp < periodYearIni) {
              ArrReturn[cont] = arrAuxiliar;
              cont++;
            }
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

    return {
      getInputData: getInputData,
      map: map,
      summarize: summarize
    };

  });
