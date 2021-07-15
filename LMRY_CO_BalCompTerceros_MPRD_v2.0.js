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
define(['N/search', 'N/log', 'require', 'N/file', 'N/runtime', 'N/query',"N/format", "N/record", "N/task", "N/config", "./CO_Library_Mensual/LMRY_CO_Reportes_LBRY_V2.0.js"],

    function(search, log, require, fileModulo, runtime, query, format, recordModulo, task, config, libreria) {

        /**
         * Input Data for processing
         *
         * @return Array,Object,Search,File
         *
         * @since 2016.1
         */

        var objContext = runtime.getCurrentScript();

        var LMRY_script = "LMRY_CO_BalCompTerceros_MPRDC.js";

        var paramSubsidy = null;    // Adistec Colombia
        var paramPeriod = null;     // Marzo 2013
        var paramPeriodFin = null;
        var paramMultibook = null;
        var paramEntity = null;
        var paramRecordID = null;
        var paramPUC = null;

        var ArrYears = new Array();
        var ArrProcessedYears = new Array();
        var ArrData = new Array();
        var ArrDataRestante = new Array();
        var ArrDataRestanteSpecific = new Array();
        var ArrDataActual = new Array();
        var ArrDataAcualSpecific = new Array();
        var ArrAllPeriods = new Array();
        var ArrYearPeriods = new Array();
        var ArrEntidades = new Array();

        var periodYearIni;
        var periodMonthIni;
        var periodIniIsAdjust = false;

        var periodYearFin;
        var periodMonthFin;
        var periodstartdateIni;
        var periodstartdateFin;

        var featuresubs;
        var featurejobs;
        var feamultibook;

        var entityCustomer = false;
        var entityVendor = false;
        var entityEmployee = false;
        var entityOtherName = false;

        var entity_name;
        var entity_id;
        var entity_nit;

        function getInputData() {
            try{
                log.error('getInputData', 'getInputData');

                ParametrosYFeatures();

                // Obtiene años ya procesados
                ArrProcessedYears = ObtenerAñosProcesados();
                // Obtiene los periodos Fiscal Year
                ArrYears = ObtenerAñosFiscales();

                OrdenarAños(ArrYears);

                OrdenarAños(ArrProcessedYears);

                if(paramEntity != null){
                    ObtenerEntidad(paramEntity);
                }

                for(var i = 0; i < ArrYears.length; i++){
                    var flag = false;
                    
                    for(var j = 0; j < ArrProcessedYears.length; j++){
                        if(ArrProcessedYears[j][1] == ArrYears[i][1] && ArrProcessedYears[j][4] == paramPUC){
                            flag = true;
                            break;
                        }
                    }

                    if(!flag){                        
                        var arrTemporal = new Array();
                        
                        var arrTemporalSpecific = new Array();

                        arrTemporal = ObtenerData(ArrYears[i][0], true,false, true);

                        var configpage = config.load({
                            type: config.Type.COMPANY_INFORMATION
                        });
                        
                        if (featuresubs || featuresubs == 'T'){
                            
                        } else {
                            var idProce = configpage.getValue('id');
                        }

                        //Obtiene Specific Transactions
                        if(feamultibook){
                            arrTemporalSpecific = ObtenerData(ArrYears[i][0], true, true, true);

                            Array.prototype.push.apply(arrTemporal, arrTemporalSpecific);
                        }

                        if(arrTemporal.length != 0){
                            arrTemporal = AgruparPorCuenta(arrTemporal);

                            for(var x = 0; x < arrTemporal.length; x++){
                                arrTemporal[x][4] = ArrYears[i][1];

                                if(featuresubs){
                                    arrTemporal[x][5] = paramSubsidy;
                                }

                                ArrData.push(arrTemporal[x]);
                            }
                        }

                        var record = recordModulo.create({
                            type: 'customrecord_lmry_co_terceros_procesados',
                        });

                        if(featuresubs || featuresubs == 'T'){
                            record.setValue({
                                fieldId: 'custrecord_lmry_co_subsi_procesado',
                                value: '' + paramSubsidy
                            });
                        } else {
                            record.setValue({
                                fieldId: 'custrecord_lmry_co_subsi_procesado',
                                value: '' + idProce
                            });
                        }

                        record.setValue({
                            fieldId: 'custrecord_lmry_co_year_procesado',
                            value: ArrYears[i][1]
                        });

                        record.setValue({
                            fieldId: 'custrecord_lmry_co_puc_procesado',
                            value: '' + paramPUC
                        });

                        if(feamultibook || feamultibook == 'T'){
                            record.setValue({
                                fieldId: 'custrecord_lmry_co_multibook_procesado',
                                value: '' + paramMultibook
                            });
                        }

                        record.save();
                    }
                }
                //log.error('ArrData',ArrData);
                return ArrData;
                
            }catch(err){
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
            try{
                paramPUC = objContext.getParameter({
                    name: 'custscript_lmry_terceros_mprdc_lastpuc'
                });

                if(paramPUC == '' || paramPUC == null){
                    paramPUC = 1;
                }

                var arrTemp = JSON.parse(context.value);

                var record = recordModulo.create({
                    type: 'customrecord_lmry_co_terceros_data',
                });

                var account_lookup = search.lookupFields({
                    type: search.Type.ACCOUNT,
                    id: Number(arrTemp[0]),
                    columns: ['custrecord_lmry_co_puc_d6_id']
                });

                if(((account_lookup.custrecord_lmry_co_puc_d6_id)[0].text).charAt(0) == paramPUC){

                    // 7. PUC 6
                    record.setValue({
                        fieldId: 'custrecord_lmry_co_terceros_puc6',
                        value: '' + (account_lookup.custrecord_lmry_co_puc_d6_id)[0].text
                    });

                    // var json_account = {};

                    // if(account_lookup != null){
                    //     json_account.puc6 = account_lookup.custrecord_lmry_co_puc_d6_id;
                    //     json_account.desc6 = account_lookup.custrecord_lmry_co_puc_d6_description;
                    //     json_account.puc4 = account_lookup.custrecord_lmry_co_puc_d4_id;
                    //     json_account.desc4 = account_lookup.custrecord_lmry_co_puc_d4_description;
                    //     json_account.puc2 = account_lookup.custrecord_lmry_co_puc_d2_id;
                    //     json_account.desc2 = account_lookup.custrecord_lmry_co_puc_d2_description;
                    //     json_account.puc1 = account_lookup.custrecord_lmry_co_puc_d1_id;
                    //     json_account.desc1 = account_lookup.custrecord_lmry_co_puc_d1_description;
                    // }

                    // arrTemp[0] = JSON.stringify(json_account);

                    // 0. Account
                    record.setValue({
                        fieldId: 'custrecord_lmry_co_terceros_account',
                        value: arrTemp[0]
                    });

                    // 1. Debit
                    if(arrTemp[1] != null && arrTemp[1] != ''){
                        record.setValue({
                            fieldId: 'custrecord_lmry_co_terceros_debit',
                            value: arrTemp[1]
                        });
                    }else{
                        record.setValue({
                            fieldId: 'custrecord_lmry_co_terceros_debit',
                            value: 0
                        });
                    }

                    // 2. Credit
                    if(arrTemp[2] != null && arrTemp[2] != ''){
                        record.setValue({
                            fieldId: 'custrecord_lmry_co_terceros_credit',
                            value: arrTemp[2]
                        });
                    }else{
                        record.setValue({
                            fieldId: 'custrecord_lmry_co_terceros_credit',
                            value: 0
                        });
                    }

                    // 3. Entity
                    var json_entity = {};

                    var flag_entity = ObtenerEntidad(arrTemp[3]);

                    if(flag_entity){
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

                    feamultibook = runtime.isFeatureInEffect({
                        feature: "MULTIBOOK"
                    });

                    // 5. Multibook
                    if(feamultibook || feamultibook == 'T'){
                        paramMultibook = objContext.getParameter({
                            name: 'custscript_lmry_terceros_mprdc_multi'
                        });

                        record.setValue({
                            fieldId: 'custrecord_lmry_co_terceros_multibook',
                            value: '' + paramMultibook
                        });
                    }

                    featuresubs = runtime.isFeatureInEffect({
                        feature: "SUBSIDIARIES"
                    });

                    // 6. Subsidiary
                    if(featuresubs || featuresubs == 'T'){
                        paramSubsidy = objContext.getParameter({
                            name: 'custscript_lmry_terceros_mprdc_subsi'
                        });

                        record.setValue({
                            fieldId: 'custrecord_lmry_co_terceros_subsi',
                            value: '' + paramSubsidy
                        });
                    }

                    var id = record.save();
                }
            }catch(err){
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
            try{
                log.error('summarize', 'summarize');

                ParametrosYFeatures();

                ArrEntidades = ObtenerEntidades();

                // Obtiene los periodos Fiscal Year
                ArrYears = ObtenerAñosFiscales();

                OrdenarAños(ArrYears);

                // Obtener todos los periodos
                ArrAllPeriods = ObtenerPeriodos();

                // Obtener periodos del año
                ArrYearPeriods = ObtenerPeriodosDelAño(ArrAllPeriods);

                var ArrYearsForSearch = new Array();
                var cont = 0;

                for(var i = 0; i < ArrYears.length; i++){
                    if(ArrYears[i][1] < periodYearIni){
                        ArrYearsForSearch[cont] = ArrYears[i][1];
                        cont++;
                    }
                }

                if(paramEntity != null){
                    ObtenerEntidad(paramEntity);
                }
                if(ArrYearsForSearch.length != 0){
                    ArrData = ObtenerDataDeRecord(ArrYearsForSearch[0],ArrYearsForSearch[ArrYearsForSearch.length - 1]);
                }
                    // Obtiene data de los periodos restantes
                    if(ArrYearPeriods.length != 0){
                        ArrDataRestante = ObtenerData(ArrYearPeriods,false, false, false);
                        if(feamultibook){
                            ArrDataRestanteSpecific = ObtenerData(ArrYearPeriods,false, true, false);

                            if(ArrDataRestanteSpecific.length != 0){
                                //ArrDataRestante = JuntarConDataSpecific(ArrDataRestante, ArrDataRestanteSpecific);
                                Array.prototype.push.apply(ArrDataRestante, ArrDataRestanteSpecific);
                            }
                        }

                        ArrDataRestante = EntityToJson(ArrDataRestante);

                        if(ArrDataRestante.length != 0){
                            ArrDataRestante = AgruparPorCuenta(ArrDataRestante);
                        }
                        //TODO EL SALDO ANTERIOR
                        ArrData = JuntarYAgruparArreglos(ArrData, ArrDataRestante);
                    }
                

                //ArrData es Saldo Anterior

                // Obtiene Movimientos
                if(paramPeriodFin != null){
                    ArrDataActual = ObtenerData(paramPeriod, true, false, false, paramPeriodFin);
                }else{
                    ArrDataActual = ObtenerData(paramPeriod, true, false, false);
                }

                if(feamultibook){
                    if(paramPeriodFin != null){
                        ArrDataAcualSpecific = ObtenerData(paramPeriod,true, true, false, paramPeriodFin);
                    }else{
                        ArrDataAcualSpecific = ObtenerData(paramPeriod,true, true, false);
                    }

                    if(ArrDataAcualSpecific.length != 0){
                        Array.prototype.push.apply(ArrDataActual, ArrDataAcualSpecific);
                    }
                }

                ArrDataActual = EntityToJson(ArrDataActual);

                if(ArrDataActual.length != 0){
                    ArrDataActual = AgruparPorCuenta(ArrDataActual);
                }

                ArrData = ObtenerArregloFinalSeisDigitos(ArrData, ArrDataActual);

                ArrData_str = ConvertirAString(ArrData);
                
                var idfile = savefile(ArrData_str);

                LanzarSchedule(idfile);

            }catch(err){
                log.error('err', err);
                libreria.sendMail(LMRY_script, ' [ getInputData ] ' + err);
            }
        }

        function EntityToJson(ArrTemp) {
            for(var i = 0; i < ArrTemp.length; i++){
                if(ArrTemp[i][3] != null && ArrTemp[i][3] != ''){
                    for(var j = 0; j < ArrEntidades.length; j++){
                        if(ArrTemp[i][3] == ArrEntidades[j][0]){
                            var json_entity = {};

                            json_entity.name = ArrEntidades[j][1];
                            
                            json_entity.nit = ArrEntidades[j][2];

                            json_entity.internalid = ArrEntidades[j][0];
                            
                            ArrTemp[i][3] = JSON.stringify(json_entity);

                            break;
                        }
                        //No encuentra la entidad
                        if(j == ArrEntidades.length - 1){
                            ArrTemp[i][3] = '';
                        }
                    }
                }
            }

            return ArrTemp;
        }

        function ObtenerEntidades() {
            var intDMinReg = 0;
            var intDMaxReg = 1000;

            var DbolStop = false;
            var ArrVendors_temp = new Array();
            var cont = 0;

            //Obtiene Vendors
            var busqueda_vendor = search.create({
                type: 'vendor',
                columns: [
                    search.createColumn({
                        name: 'isperson'
                    }),
                    search.createColumn({
                        name: 'firstname'
                    }),
                    search.createColumn({
                        name: 'lastname'
                    }),
                    search.createColumn({
                        name: 'companyname'
                    }),
                    search.createColumn({
                        name: 'vatregnumber'
                    }),
                    search.createColumn({
                        name: 'internalid'
                    })
                ]
            });

            var savedsearch_vendor = busqueda_vendor.run();

            while (!DbolStop) {
                var objResult = savedsearch_vendor.getRange(intDMinReg, intDMaxReg);

                if (objResult != null) {

                    if (objResult.length != 1000) {
                        DbolStop = true;
                    }

                    for (var i = 0; i < objResult.length; i++) {
                        var columns = objResult[i].columns;

                        var arrAuxiliar = new Array();

                        // 0 Internal ID
                        if (objResult[i].getValue(columns[5])!=null  && objResult[i].getValue(columns[5])!='- None -' && objResult[i].getValue(columns[5])!='NaN' && objResult[i].getValue(columns[5])!='undefined'){
                            arrAuxiliar[0] = objResult[i].getValue(columns[5]);
                        } else {
                            arrAuxiliar[0] = ''
                        }
                        
                        // 1. Name
                        if(objResult[i].getValue(columns[0]) || objResult[i].getValue(columns[0]) == 'T'){
                            arrAuxiliar[1] = '' + objResult[i].getValue(columns[1]) + ' ' + objResult[i].getValue(columns[2]);
                        }else{
                            arrAuxiliar[1] = objResult[i].getValue(columns[3]);
                        }

                        // 2. NIT
                        if (objResult[i].getValue(columns[4])!=null  && objResult[i].getValue(columns[4])!='- None -' && objResult[i].getValue(columns[4])!='NaN' && objResult[i].getValue(columns[4])!='undefined'){
                            arrAuxiliar[2] = '' + objResult[i].getValue(columns[4]);
                        } else {
                            arrAuxiliar[2] = '';
                        }

                        ArrVendors_temp[cont] = arrAuxiliar;
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

            //Obtiene Customers
            intDMinReg = 0;
            intDMaxReg = 1000;

            DbolStop = false;
            var ArrCustomers_temp = new Array();
            cont = 0;

            var busqueda_customer = search.create({
                type: 'customer',
                columns: [
                    search.createColumn({
                        name: 'isperson'
                    }),
                    search.createColumn({
                        name: 'firstname'
                    }),
                    search.createColumn({
                        name: 'lastname'
                    }),
                    search.createColumn({
                        name: 'companyname'
                    }),
                    search.createColumn({
                        name: 'vatregnumber'
                    }),
                    search.createColumn({
                        name: 'internalid'
                    })
                ]
            });

            var savedsearch_customer = busqueda_customer.run();

            while (!DbolStop) {
                var objResult = savedsearch_customer.getRange(intDMinReg, intDMaxReg);

                if (objResult != null) {

                    if (objResult.length != 1000) {
                        DbolStop = true;
                    }

                    for (var i = 0; i < objResult.length; i++) {
                        var columns = objResult[i].columns;

                        var arrAuxiliar = new Array();

                        // 0. Internal ID
                        if (objResult[i].getValue(columns[5])!=null  && objResult[i].getValue(columns[5])!='- None -' && objResult[i].getValue(columns[5])!='NaN' && objResult[i].getValue(columns[5])!='undefined'){
                            arrAuxiliar[0] = objResult[i].getValue(columns[5]);
                        }else{
                            arrAuxiliar[0] = '';
                        }
                        
                        // 1. Name
                        if(objResult[i].getValue(columns[0]) || objResult[i].getValue(columns[0]) == 'T'){
                            arrAuxiliar[1] = '' + objResult[i].getValue(columns[1]) + ' ' + objResult[i].getValue(columns[2]);
                        }else{
                            arrAuxiliar[1] = objResult[i].getValue(columns[3]);
                        }

                        // 2. NIT
                        if (objResult[i].getValue(columns[4])!=null  && objResult[i].getValue(columns[4])!='- None -' && objResult[i].getValue(columns[4])!='NaN' && objResult[i].getValue(columns[4])!='undefined'){
                            arrAuxiliar[2] = '' + objResult[i].getValue(columns[4]);
                        }else{
                            arrAuxiliar[2] = '';
                        }
                        
                        ArrCustomers_temp[cont] = arrAuxiliar;
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

            //Obtiene Employees
            intDMinReg = 0;
            intDMaxReg = 1000;

            DbolStop = false;
            var ArrEmployees_temp = new Array();
            cont = 0;

            var busqueda_employee = search.create({
                type: 'employee',
                columns: [
                    search.createColumn({
                        name: 'firstname'
                    }),
                    search.createColumn({
                        name: 'lastname'
                    }),
                    search.createColumn({
                        name: 'custentity_lmry_sv_taxpayer_number'
                    }),
                    search.createColumn({
                        name: 'internalid'
                    })
                ]
            });

            var savedsearch_employee = busqueda_employee.run();

            while (!DbolStop) {
                var objResult = savedsearch_employee.getRange(intDMinReg, intDMaxReg);

                if (objResult != null) {

                    if (objResult.length != 1000) {
                        DbolStop = true;
                    }

                    for (var i = 0; i < objResult.length; i++) {
                        var columns = objResult[i].columns;

                        var arrAuxiliar = new Array();

                        // 0. Internal ID
                        if (objResult[i].getValue(columns[3])!=null  && objResult[i].getValue(columns[3])!='- None -' && objResult[i].getValue(columns[3])!='NaN' && objResult[i].getValue(columns[3])!='undefined'){
                            arrAuxiliar[0] = '' + objResult[i].getValue(columns[3]);
                        }else{
                            arrAuxiliar[0] = '';
                        }

                        // 1. Name
                        arrAuxiliar[1] = '' + objResult[i].getValue(columns[0]) + ' ' + objResult[i].getValue(columns[1]);

                        // 2. NIT
                        if (objResult[i].getValue(columns[2])!=null  && objResult[i].getValue(columns[2])!='- None -' && objResult[i].getValue(columns[2])!='NaN' && objResult[i].getValue(columns[2])!='undefined'){
                            arrAuxiliar[2] = '' + objResult[i].getValue(columns[2]);
                        }else{
                            arrAuxiliar[2] = '';
                        }
                        

                        ArrEmployees_temp[cont] = arrAuxiliar;
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

            //Obtiene Other Name
            intDMinReg = 0;
            intDMaxReg = 1000;

            DbolStop = false;
            var ArrOtherName_temp = new Array();
            cont = 0;

            var busqueda_other_name = search.create({
                type: "othername",
                fiters: [
                    search.createFilter({
                        name: 'subsidiary',
                        operator: search.Operator.ANYOF,
                        value: paramSubsidy
                    })
                ],
                columns: [
                    search.createColumn({
                        name: 'internalid'
                    })
                ]
            });

            var savedsearch_other_name = busqueda_other_name.run();

            while (!DbolStop) {
                var objResult = savedsearch_other_name.getRange(intDMinReg, intDMaxReg);

                if (objResult != null) {

                    if (objResult.length != 1000) {
                        DbolStop = true;
                    }

                    for (var i = 0; i < objResult.length; i++) {
                        var columns = objResult[i].columns;

                        // 0 Internal ID
                        ArrOtherName_temp.push(objResult[i].getValue(columns[0]));
                    }

                    if (!DbolStop) {
                        intDMinReg = intDMaxReg;
                        intDMaxReg += 1000;
                    }

                } else {
                    DbolStop = true;
                }
            }

            var ArrOtherNameFinal = [];

            for(var i = 0; i < ArrOtherName_temp.length; i++){
                var otherNameRcd = recordModulo.load({
                    type: search.Type.OTHER_NAME,
                    id: ArrOtherName_temp[i]
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

                var arrTemp = [];

                arrTemp[0] = ArrOtherName_temp[i];

                if(ispersonField && ispersonField != 'F'){
                    arrTemp[1] = firstnameField + ' ' + lastnameField;
                }else{
                    arrTemp[1] = companynameField;
                }

                arrTemp[2] = vatregnumberField;

                ArrOtherNameFinal.push(arrTemp);
            }

            //Juntar Arreglos
            var ArrReturn = ArrEmployees_temp.concat(ArrCustomers_temp.concat(ArrVendors_temp.concat(ArrOtherNameFinal)));

            return ArrReturn;
        }

        function AgruparPorCuenta(ArrTemp){
            var ArrReturn = new Array();

            ArrReturn.push(ArrTemp[0]);

            for(var i = 1; i < ArrTemp.length; i++){
                var intLength = ArrReturn.length;
                for(var j = 0; j < intLength; j++){
                   //if(ArrTemp[i][0] == ArrReturn[j][0] && ArrTemp[i][3].trim() == ArrReturn[j][3].trim()){
                    if(ArrTemp[i][0] == ArrReturn[j][0] && ArrTemp[i][3] == ArrReturn[j][3]){
                       ArrReturn[j][1] = Math.abs(ArrReturn[j][1]) + Math.abs(ArrTemp[i][1]);
                       ArrReturn[j][2] = Math.abs(ArrReturn[j][2]) + Math.abs(ArrTemp[i][2]);
                       break;
                   }
                   if(j == ArrReturn.length - 1){
                       ArrReturn.push(ArrTemp[i]);
                   }
               }
            }

            return ArrReturn;
        }

        function JuntarConDataSpecific(ArrDataRestante, ArrDataRestanteSpecific) {
            var Length = ArrDataRestante.length;
            for(var i = 0; i < ArrDataRestanteSpecific.length; i++){
                for(var j = 0; j < Length; j++){
                    if(ArrDataRestante[j][0] == ArrDataRestanteSpecific[i][0] && ArrDataRestante[j][3].trim() == ArrDataRestanteSpecific[i][3].trim()){
                        ArrDataRestante[j][1] = Number(ArrDataRestante[j][1]) + Number(ArrDataRestanteSpecific[i][1]);
                        ArrDataRestante[j][2] = Number(ArrDataRestante[j][2]) + Number(ArrDataRestanteSpecific[i][2]);
                        break;
                    }
                    if(j == Length - 1){
                        ArrDataRestante.push(ArrDataRestanteSpecific[i]);
                    }
                }
            }

            return ArrDataRestante;
        }

        function ObtenerEntidad(paramEntity){
            try{
                if(paramEntity != null && paramEntity != ''){
                    var entity_customer_temp = search.lookupFields({
                        type: search.Type.CUSTOMER,
                        id: Number(paramEntity),
                        columns: ['entityid','firstname','lastname', 'companyname','internalid','vatregnumber']
                    });

                    var entity_id;

                    entity_nit = entity_customer_temp.vatregnumber;

                    if(entity_customer_temp.internalid != null){
                        entity_id = (entity_customer_temp.internalid)[0].value;
                    }
                    
                    entity_name = entity_customer_temp.firstname + ' ' + entity_customer_temp.lastname;

                    if((entity_customer_temp.firstname == null || entity_customer_temp.firstname == '') && (entity_customer_temp.lastname == null || entity_customer_temp.lastname == '') && entity_name.trim() == ''){
                        entity_name = entity_customer_temp.companyname;

                        if(entity_name == null && entity_name.trim() == ''){
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
                            columns: ['entityid','firstname','lastname', 'companyname', 'internalid', 'vatregnumber']
                        });

                        entity_nit = entity_vendor_temp.vatregnumber;

                        if(entity_vendor_temp.internalid != null){
                            entity_id = (entity_vendor_temp.internalid)[0].value;
                        }
                        
                        entity_name = entity_vendor_temp.firstname + ' ' + entity_vendor_temp.lastname;

                        if((entity_vendor_temp.firstname == null || entity_vendor_temp.firstname == '') && (entity_vendor_temp.lastname == null || entity_vendor_temp.lastname == '') && entity_name.trim() == ''){
                            entity_name = entity_vendor_temp.companyname;

                            if(entity_name == null && entity_name.trim() == ''){
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
                                columns: ['entityid','firstname','lastname', 'internalid', 'custentity_lmry_sv_taxpayer_number']
                            });

                            entity_nit = entity_employee_temp.custentity_lmry_sv_taxpayer_number;

                            if(entity_employee_temp.internalid != null){
                                entity_id = (entity_employee_temp.internalid)[0].value;
                            }
                            
                            entity_name = entity_employee_temp.firstname + ' ' + entity_employee_temp.lastname;

                            if(entity_name == null && entity_name.trim() == ''){
                                entity_name = entity_employee_temp.entityid;
                            }

                            if (entity_id != null) {
                                entityEmployee = true;
                                return true;
                            }else{
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

                                if(internalidField != null){
                                    entity_id = internalidField;
                                }

                                if(ispersonField == true || ispersonField == 'T'){
                                    entity_name = firstnameField + ' ' + lastnameField;
                                }else{
                                    entity_name = companynameField;
                                }

                                if(entity_id != null){
                                    entityOtherName = true;
                                    return true;
                                }else{
                                    return false;
                                }
                            }
                        }
                    }
                }else{
                    return false;
                }
            }catch(err){
                log.error('err', err);
                
                log.error('paramEntity', paramEntity);

                return false;
            }
        }

        function ConvertirAString(ArrTemp){
            var str_return = '';

            for(var i = 0; i < ArrTemp.length; i++){
                for(var j = 0; j < ArrTemp[i].length; j++){
                    str_return += ArrTemp[i][j];
                    str_return += '|';
                }
                str_return += '\r\n';
            }

            return str_return;
        }

        function LanzarSchedule(idfile){
            var params = {};

            paramRecordID = objContext.getParameter({
                    name: 'custscript_lmry_terceros_mprdc_record'
            });

            paramEntity = objContext.getParameter({
                    name: 'custscript_lmry_terceros_mprdc_entity'
            });

            paramMultibook = objContext.getParameter({
                    name: 'custscript_lmry_terceros_mprdc_multi'
            });

            paramSubsidy = objContext.getParameter({
                    name: 'custscript_lmry_terceros_mprdc_subsi'
            });

            paramPeriod = objContext.getParameter({
                    name: 'custscript_lmry_terceros_mprdc_period'
            });

            paramPeriodFin = objContext.getParameter({
                    name: 'custscript_lmry_terceros_mprdc_periodFin'
            });

            //params['custscript_5'] = paramRecordID;

            /*paramPeriod = 303;
            paramSubsidy = 16;
            paramMultibook = 1;
            paramEntity = null;*/

            params['custscript_lmry_co_terce_schdl_recordid'] = paramRecordID;            

            params['custscript_lmry_co_terce_schdl_period'] = paramPeriod;

            params['custscript_lmry_co_terce_schdl_fileid'] = idfile;

            params['custscript_lmry_co_terce_schdl_lastpuc'] = paramPUC;

            if(featuresubs){
                params['custscript_lmry_co_terce_schdl_subsi'] = paramSubsidy;
            }

            if(feamultibook){
                params['custscript_lmry_co_terce_schdl_multi'] = paramMultibook
            }

            if(paramEntity != null){
                params['custscript_lmry_co_terce_schdl_entity'] = paramEntity;
            }

            if(paramPeriodFin != null){
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

        function savefile(Final_string) {
            var FolderId = objContext.getParameter({
                name: 'custscript_lmry_file_cabinet_rg_co'
            });

            // Almacena en la carpeta de Archivos Generados
            if (FolderId != '' && FolderId != null) {
                var Final_NameFile = 'TERCEROS_ADISTEC_MAPREDUCE_TEST' + '.txt';
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

        function ObtenerArregloFinalSeisDigitos(ArrSaldoAnterior, ArrMovimientos) {
            /*
             * SEARCH
             * 0.  cuenta
             * 1.  denominacion
             * 2.  sum debitos
             * 3.  sum credito
             * 4.  cuenta 4 digitos
             * 5.  denominacion 4 digitos
             * 6.  cuenta 2 digitos
             * 7.  denominacion 2 digitos
             * 8.  cuenta 1 digito
             * 9.  denominacion 1 digito
             * 10. entidad
             */

            var arr_final = new Array();
            var cont = 0;

            if (ArrSaldoAnterior != null && ArrSaldoAnterior.length != 0 && ArrMovimientos != null && ArrMovimientos.length != 0) {
                for (var i = 0; i < ArrSaldoAnterior.length; i++) {
                    for (var j = 0; j < ArrMovimientos.length; j++) {
                        if (ArrSaldoAnterior[i][0] == ArrMovimientos[j][0] && ArrSaldoAnterior[i][3].trim() == ArrMovimientos[j][3].trim()) {
                            var sub_array = new Array();
                            
                            sub_array[0] = ArrSaldoAnterior[i][0];

                            var saldo_anterior = Math.abs(Number(ArrSaldoAnterior[i][1])) - Math.abs(Number(ArrSaldoAnterior[i][2]));

                            if (saldo_anterior > 0) {
                                sub_array[1] = Math.abs(saldo_anterior);
                                sub_array[2] = 0.0;
                            } else if (saldo_anterior < 0) {
                                sub_array[1] = 0.0;
                                sub_array[2] = Math.abs(saldo_anterior);
                            } else if (saldo_anterior == 0) {
                                sub_array[1] = 0.0;
                                sub_array[2] = 0.0;
                            }

                            var movimientos = Math.abs(Number(ArrMovimientos[j][1])) - Math.abs(Number(ArrMovimientos[j][2]));

                            /*if (movimientos > 0) {
                                sub_array[3] = Math.abs(movimientos);
                                sub_array[4] = 0.0;
                            } else if (movimientos < 0) {
                                sub_array[3] = 0.0;
                                sub_array[4] = Math.abs(movimientos);
                            } else if (movimientos == 0) {
                                sub_array[3] = 0.0;
                                sub_array[4] = 0.0;
                            }*/

                            sub_array[3] = Math.abs(Number(ArrMovimientos[j][1]));
                            sub_array[4] = Math.abs(Number(ArrMovimientos[j][2]));

                            var nuevoSaldo = Math.abs(Number(ArrSaldoAnterior[i][1])) - Math.abs(Number(ArrSaldoAnterior[i][2])) + Math.abs(Number(ArrMovimientos[j][1])) - Math.abs(Number(ArrMovimientos[j][2]));

                            if (nuevoSaldo > 0) {
                                sub_array[5] = Math.abs(nuevoSaldo);
                                sub_array[6] = 0.0;
                            } else if (nuevoSaldo < 0) {
                                sub_array[5] = 0.0;
                                sub_array[6] = Math.abs(nuevoSaldo);
                            } else if (nuevoSaldo == 0) {
                                sub_array[5] = 0.0;
                                sub_array[6] = 0.0;
                            }

                            sub_array[7] = ArrSaldoAnterior[i][3];

                            if (!(Math.abs(ArrSaldoAnterior[i][1]) == 0 && Math.abs(ArrSaldoAnterior[i][2]) == 0 &&
                                    Math.abs(ArrMovimientos[j][1]) == 0 && Math.abs(ArrMovimientos[j][2]) == 0)) {
                                arr_final[cont] = sub_array;
                                cont++;
                            }

                            break;
                        } else {
                            if (j == ArrMovimientos.length - 1) {
                                var sub_array_2 = new Array();
                                sub_array_2[0] = ArrSaldoAnterior[i][0];

                                sub_array_2[1] = 0.0;
                                sub_array_2[2] = 0.0;
                                sub_array_2[3] = 0.0;
                                sub_array_2[4] = 0.0;

                                var saldo_anterior = Math.abs(Number(ArrSaldoAnterior[i][1])) - Math.abs(Number(ArrSaldoAnterior[i][2]));

                                if (saldo_anterior > 0) {
                                    sub_array_2[1] = Math.abs(saldo_anterior);
                                    sub_array_2[2] = 0.0;
                                } else if (saldo_anterior < 0) {
                                    sub_array_2[1] = 0.0;
                                    sub_array_2[2] = Math.abs(saldo_anterior);
                                } else if (saldo_anterior == 0) {
                                    sub_array_2[1] = 0.0;
                                    sub_array_2[2] = 0.0;
                                }

                                sub_array_2[3] = 0.0;
                                sub_array_2[4] = 0.0;

                                var nuevoSaldo = Math.abs(Number(ArrSaldoAnterior[i][1])) - Math.abs(Number(ArrSaldoAnterior[i][2]));

                                if (nuevoSaldo > 0) {
                                    sub_array_2[5] = Math.abs(nuevoSaldo);
                                    sub_array_2[6] = 0.0;
                                } else if (nuevoSaldo < 0) {
                                    sub_array_2[5] = 0.0;
                                    sub_array_2[6] = Math.abs(nuevoSaldo);
                                } else if (nuevoSaldo == 0) {
                                    sub_array_2[5] = 0.0;
                                    sub_array_2[6] = 0.0;
                                }

                                sub_array_2[7] = ArrSaldoAnterior[i][3];
                                

                                if (Number(ArrSaldoAnterior[i][1]) != 0 || Number(ArrSaldoAnterior[i][2]) != 0) {
                                    arr_final[cont] = sub_array_2;
                                    cont++;
                                }

                                break;
                            }
                        }
                    }
                }

                for (var i = 0; i < ArrMovimientos.length; i++) {
                    for (var j = 0; j < ArrSaldoAnterior.length; j++) {
                        if (ArrSaldoAnterior[j][0] == ArrMovimientos[i][0]) {
                            if (ArrSaldoAnterior[j][3].trim() != ArrMovimientos[i][3].trim()) {
                                if (j == ArrSaldoAnterior.length - 1) {
                                    var sub = new Array();
                                    sub[0] = ArrMovimientos[i][0];

                                    sub[1] = 0.0;
                                    sub[2] = 0.0;

                                    var movimientos = Math.abs(Number(ArrMovimientos[i][1])) - Math.abs(Number(ArrMovimientos[i][2]));

                                    /*if (movimientos > 0) {
                                        sub[3] = Math.abs(movimientos);
                                        sub[4] = 0.0;
                                    } else if (movimientos < 0) {
                                        sub[3] = 0.0;
                                        sub[4] = Math.abs(movimientos);
                                    } else if (movimientos == 0.0) {
                                        sub[3] = 0.0;
                                        sub[4] = 0.0;
                                    }*/

                                    sub[3] = Math.abs(Number(ArrMovimientos[i][1]));
                                    sub[4] = Math.abs(Number(ArrMovimientos[i][2]));

                                    var nuevo_saldo = Math.abs(Number(ArrMovimientos[i][1])) - Math.abs(Number(ArrMovimientos[i][2]));

                                    if (nuevo_saldo > 0) {
                                        sub[5] = Math.abs(nuevo_saldo);
                                        sub[6] = 0.0;
                                    } else if (nuevo_saldo < 0) {
                                        sub[5] = 0.0;
                                        sub[6] = Math.abs(nuevo_saldo);
                                    } else if (nuevo_saldo == 0.0) {
                                        sub[5] = 0.0;
                                        sub[6] = 0.0;
                                    }

                                    sub[7] = ArrMovimientos[i][3];
                                    
                                    if (Math.abs(Number(ArrMovimientos[i][1])) != 0 || Math.abs(Number(ArrMovimientos[i][2])) != 0) {
                                        arr_final[cont] = sub;
                                        cont++;
                                    }

                                    break;
                                }
                            } else {
                                break;
                            }
                        } else {
                            if (j == ArrSaldoAnterior.length - 1) {
                                var sub = new Array();
                                sub[0] = ArrMovimientos[i][0];

                                sub[1] = 0.0;
                                sub[2] = 0.0;

                                var movimientos = Math.abs(Number(ArrMovimientos[i][1])) - Math.abs(Number(ArrMovimientos[i][2]));

                                /*if (movimientos > 0) {
                                    sub[3] = Math.abs(movimientos);
                                    sub[4] = 0.0;
                                } else if (movimientos < 0) {
                                    sub[3] = 0.0;
                                    sub[4] = Math.abs(movimientos);
                                } else if (movimientos == 0) {
                                    sub[3] = 0.0;
                                    sub[4] = 0.0;
                                }*/

                                sub[3] = Math.abs(Number(ArrMovimientos[i][1]));
                                sub[4] = Math.abs(Number(ArrMovimientos[i][2]));

                                var nuevo_saldo = Math.abs(Number(ArrMovimientos[i][1])) - Math.abs(Number(ArrMovimientos[i][2]));

                                if (nuevo_saldo > 0) {
                                    sub[5] = Math.abs(nuevo_saldo);
                                    sub[6] = 0.0;
                                } else if (nuevo_saldo < 0) {
                                    sub[5] = 0.0;
                                    sub[6] = Math.abs(nuevo_saldo);
                                } else if (nuevo_saldo == 0.0) {
                                    sub[5] = 0.0;
                                    sub[6] = 0.0;
                                }
                                sub[7] = ArrMovimientos[i][3];
                                
                                if (Math.abs(Number(ArrMovimientos[i][1])) != 0 || Math.abs(Number(ArrMovimientos[i][2])) != 0) {
                                    arr_final[cont] = sub;
                                    cont++;
                                }

                                break;
                            }
                        }
                    }
                }
            } else if (ArrMovimientos != null && ArrMovimientos.length != 0) {
                for (var i = 0; i < ArrMovimientos.length; i++) {
                    var sub = new Array();

                    sub[0] = ArrMovimientos[i][0];

                    sub[1] = 0.0;
                    sub[2] = 0.0;

                    var movimientos = Math.abs(ArrMovimientos[i][1]) - Math.abs(ArrMovimientos[i][2]);

                    /*if (movimientos > 0) {
                        sub[3] = Math.abs(movimientos);
                        sub[4] = 0.0;
                    } else if (movimientos < 0) {
                        sub[3] = 0.0;
                        sub[4] = Math.abs(movimientos);
                    } else if (movimientos == 0) {
                        sub[3] = 0.0;
                        sub[4] = 0.0;
                    }*/

                    sub[3] = Math.abs(ArrMovimientos[i][1]);
                    sub[4] = Math.abs(ArrMovimientos[i][2]);

                    var nuevo_saldo = Number(ArrMovimientos[i][1]) - Number(ArrMovimientos[i][2]);

                    if (nuevo_saldo > 0) {
                        sub[5] = Math.abs(nuevo_saldo);
                        sub[6] = 0.0;
                    } else if (nuevo_saldo < 0) {
                        sub[5] = 0.0;
                        sub[6] = Math.abs(nuevo_saldo);
                    } else if (nuevo_saldo == 0.0) {
                        sub[5] = 0.0;
                        sub[6] = 0.0;
                    }
                    sub[7] = ArrMovimientos[i][3];
                    
                    arr_final[cont] = sub;
                    cont++;
                }
            } else if (ArrSaldoAnterior != null && ArrSaldoAnterior.length != 0) {
                for (var i = 0; i < ArrSaldoAnterior.length; i++) {
                    var sub = new Array();

                    sub[0] = ArrSaldoAnterior[i][0];

                    var saldo_anterior = Number(ArrSaldoAnterior[i][1]) - Number(ArrSaldoAnterior[i][2]);

                    if (saldo_anterior > 0) {
                        sub[1] = Math.abs(saldo_anterior);
                        sub[2] = 0.0;
                    } else if (saldo_anterior < 0) {
                        sub[1] = 0.0;
                        sub[2] = Math.abs(saldo_anterior);
                    } else if (saldo_anterior == 0.0) {
                        sub[1] = 0.0;
                        sub[2] = 0.0;
                    }
                    sub[3] = 0.0;
                    sub[4] = 0.0;

                    var nuevo_saldo = Number(ArrSaldoAnterior[i][1]) - Number(ArrSaldoAnterior[i][2]);

                    if (nuevo_saldo > 0) {
                        sub[5] = Math.abs(nuevo_saldo);
                        sub[6] = 0.0;
                    } else if (nuevo_saldo < 0) {
                        sub[5] = 0.0;
                        sub[6] = Math.abs(nuevo_saldo);
                    } else if (nuevo_saldo == 0.0) {
                        sub[5] = 0.0;
                        sub[6] = 0.0;
                    }
                    sub[7] = ArrSaldoAnterior[i][3];
                    
                    arr_final[cont] = sub;
                    cont++;
                }
            } else {
                flagEmpty = true;
            }


            return arr_final;
        }

        function JuntarYAgruparArreglos(ArrData, ArrDataRestante){
            if(ArrData.length != 0 && ArrDataRestante.length != 0){
                var ArrReturn = new Array();
                var cont = 0;

                var Length = ArrData.length;

                for(var i = 0; i < ArrDataRestante.length; i++){
                    for(var j = 0; j < Length; j++){
                        if(ArrDataRestante[i][0] == ArrData[j][0] && ArrDataRestante[i][3].trim() == ArrData[j][3].trim()){
                            ArrData[j][1] = Number(ArrData[j][1]) + Number(ArrDataRestante[i][1]);
                            ArrData[j][2] = Number(ArrData[j][2]) + Number(ArrDataRestante[i][2]);
                            break;
                        }

                        if(j == Length - 1){
                            ArrData.push(ArrDataRestante[i]);
                        }
                    }
                }

                return ArrData;
            }else if(ArrData.length != 0){
                return ArrData;
            }else if(ArrDataRestante.length != 0){
                return ArrDataRestante;
            }
        }

        function ObtenerDataDeRecord(firstYear, lastYear){
            var intDMinReg = 0;
            var intDMaxReg = 1000;

            var DbolStop = false;
            var ArrReturn = new Array();
            var cont = 0;

            if(paramEntity != null){
                if(feamultibook || feamultibook == 'T'){
                    if(featuresubs || featuresubs == 'T'){
                        var busqueda = search.create({
                            type: 'customrecord_lmry_co_terceros_data',
                            filters: [
                                search.createFilter({
                                    name : 'custrecord_lmry_co_terceros_year',
                                    operator: search.Operator.GREATERTHANOREQUALTO,
                                    values: [firstYear]
                                }),
                                search.createFilter({
                                    name : 'formulanumeric',
                                    formula: 'CASE WHEN {custrecord_lmry_co_terceros_debit} - {custrecord_lmry_co_terceros_credit} <> 0 THEN 1 ELSE 0 END',
                                    operator: search.Operator.EQUALTO,
                                    values: [1]
                                }),
                                search.createFilter({
                                    name : 'custrecord_lmry_co_terceros_year',
                                    operator: search.Operator.LESSTHANOREQUALTO,
                                    values: [lastYear]
                                }),
                                search.createFilter({
                                    name : 'custrecord_lmry_co_terceros_multibook',
                                    operator: search.Operator.IS,
                                    values: [paramMultibook]
                                }),
                                search.createFilter({
                                    name : 'custrecord_lmry_co_terceros_subsi',
                                    operator: search.Operator.IS,
                                    values: [paramSubsidy]
                                }),
                                search.createFilter({
                                    name : 'custrecord_lmry_co_terceros_puc6',
                                    operator: search.Operator.STARTSWITH,
                                    values: [paramPUC]
                                })],
                            columns: [
                                search.createColumn({
                                    name: 'custrecord_lmry_co_terceros_account',
                                    summary: 'GROUP',
                                    sort: search.Sort.ASC
                                }),
                                search.createColumn({
                                    name: 'custrecord_lmry_co_terceros_debit',
                                    summary: 'SUM'
                                }),
                                search.createColumn({
                                    name: 'custrecord_lmry_co_terceros_credit',
                                    summary: 'SUM'
                                }),
                                search.createColumn({
                                    name: 'custrecord_lmry_co_terceros_entity',
                                    summary: 'GROUP'
                                })
                            ]
                        });
                    }else{
                        var busqueda = search.create({
                            type: 'customrecord_lmry_co_terceros_data',
                            filters: [
                                search.createFilter({
                                    name : 'custrecord_lmry_co_terceros_year',
                                    operator: search.Operator.GREATERTHANOREQUALTO,
                                    values: [firstYear]
                                }),
                                search.createFilter({
                                    name : 'formulanumeric',
                                    formula: 'CASE WHEN {custrecord_lmry_co_terceros_debit} - {custrecord_lmry_co_terceros_credit} <> 0 THEN 1 ELSE 0 END',
                                    operator: search.Operator.EQUALTO,
                                    values: [1]
                                }),
                                search.createFilter({
                                    name : 'custrecord_lmry_co_terceros_year',
                                    operator: search.Operator.LESSTHANOREQUALTO,
                                    values: [lastYear]
                                }),
                                search.createFilter({
                                    name : 'custrecord_lmry_co_terceros_multibook',
                                    operator: search.Operator.IS,
                                    values: [paramMultibook]
                                }),
                                search.createFilter({
                                    name : 'custrecord_lmry_co_terceros_puc6',
                                    operator: search.Operator.STARTSWITH,
                                    values: [paramPUC]
                                })],
                            columns: [
                                search.createColumn({
                                    name: 'custrecord_lmry_co_terceros_account',
                                    summary: 'GROUP',
                                    sort: search.Sort.ASC
                                }),
                                search.createColumn({
                                    name: 'custrecord_lmry_co_terceros_debit',
                                    summary: 'SUM'
                                }),
                                search.createColumn({
                                    name: 'custrecord_lmry_co_terceros_credit',
                                    summary: 'SUM'
                                }),
                                search.createColumn({
                                    name: 'custrecord_lmry_co_terceros_entity',
                                    summary: 'GROUP'
                                })
                            ]
                        });
                    }
                }else{
                    if(featuresubs || featuresubs == 'T'){
                        var busqueda = search.create({
                            type: 'customrecord_lmry_co_terceros_data',
                            filters: [
                                search.createFilter({
                                    name : 'custrecord_lmry_co_terceros_year',
                                    operator: search.Operator.GREATERTHANOREQUALTO,
                                    values: [firstYear]
                                }),
                                search.createFilter({
                                    name : 'formulanumeric',
                                    formula: 'CASE WHEN {custrecord_lmry_co_terceros_debit} - {custrecord_lmry_co_terceros_credit} <> 0 THEN 1 ELSE 0 END',
                                    operator: search.Operator.EQUALTO,
                                    values: [1]
                                }),
                                search.createFilter({
                                    name : 'custrecord_lmry_co_terceros_year',
                                    operator: search.Operator.LESSTHANOREQUALTO,
                                    values: [lastYear]
                                }),
                                search.createFilter({
                                    name : 'custrecord_lmry_co_terceros_subsi',
                                    operator: search.Operator.IS,
                                    values: [paramSubsidy]
                                }),
                                search.createFilter({
                                    name : 'custrecord_lmry_co_terceros_puc6',
                                    operator: search.Operator.STARTSWITH,
                                    values: [paramPUC]
                                })],
                            columns: [
                                search.createColumn({
                                    name: 'custrecord_lmry_co_terceros_account',
                                    summary: 'GROUP',
                                    sort: search.Sort.ASC
                                }),
                                search.createColumn({
                                    name: 'custrecord_lmry_co_terceros_debit',
                                    summary: 'SUM'
                                }),
                                search.createColumn({
                                    name: 'custrecord_lmry_co_terceros_credit',
                                    summary: 'SUM'
                                }),
                                search.createColumn({
                                    name: 'custrecord_lmry_co_terceros_entity',
                                    summary: 'GROUP'
                                })
                            ]
                        });
                    }else{
                        var busqueda = search.create({
                            type: 'customrecord_lmry_co_terceros_data',
                            filters: [
                                search.createFilter({
                                    name : 'custrecord_lmry_co_terceros_year',
                                    operator: search.Operator.GREATERTHANOREQUALTO,
                                    values: [firstYear]
                                }),
                                search.createFilter({
                                    name : 'formulanumeric',
                                    formula: 'CASE WHEN {custrecord_lmry_co_terceros_debit} - {custrecord_lmry_co_terceros_credit} <> 0 THEN 1 ELSE 0 END',
                                    operator: search.Operator.EQUALTO,
                                    values: [1]
                                }),
                                search.createFilter({
                                    name : 'custrecord_lmry_co_terceros_year',
                                    operator: search.Operator.LESSTHANOREQUALTO,
                                    values: [lastYear]
                                }),
                                search.createFilter({
                                    name : 'custrecord_lmry_co_terceros_puc6',
                                    operator: search.Operator.STARTSWITH,
                                    values: [paramPUC]
                                })],
                            columns: [
                                search.createColumn({
                                    name: 'custrecord_lmry_co_terceros_account',
                                    summary: 'GROUP',
                                    sort: search.Sort.ASC
                                }),
                                search.createColumn({
                                    name: 'custrecord_lmry_co_terceros_debit',
                                    summary: 'SUM'
                                }),
                                search.createColumn({
                                    name: 'custrecord_lmry_co_terceros_credit',
                                    summary: 'SUM'
                                }),
                                search.createColumn({
                                    name: 'custrecord_lmry_co_terceros_entity',
                                    summary: 'GROUP'
                                })
                            ]
                        });
                    }
                }
            }else{
                if(featuresubs || featuresubs == 'T'){
                    if(feamultibook || feamultibook == 'T'){
                        var busqueda = search.create({
                            type: 'customrecord_lmry_co_terceros_data',
                            filters: [
                                search.createFilter({
                                    name : 'custrecord_lmry_co_terceros_year',
                                    operator: search.Operator.GREATERTHANOREQUALTO,
                                    values: [firstYear]
                                }),
                                search.createFilter({
                                    name : 'formulanumeric',
                                    formula: 'CASE WHEN {custrecord_lmry_co_terceros_debit} - {custrecord_lmry_co_terceros_credit} <> 0 THEN 1 ELSE 0 END',
                                    operator: search.Operator.EQUALTO,
                                    values: [1]
                                }),
                                search.createFilter({
                                    name : 'custrecord_lmry_co_terceros_year',
                                    operator: search.Operator.LESSTHANOREQUALTO,
                                    values: [lastYear]
                                }),
                                search.createFilter({
                                    name : 'custrecord_lmry_co_terceros_multibook',
                                    operator: search.Operator.IS,
                                    values: [paramMultibook]
                                }),
                                search.createFilter({
                                    name : 'custrecord_lmry_co_terceros_subsi',
                                    operator: search.Operator.IS,
                                    values: [paramSubsidy]
                                }),
                                search.createFilter({
                                    name : 'custrecord_lmry_co_terceros_puc6',
                                    operator: search.Operator.STARTSWITH,
                                    values: [paramPUC]
                                })],
                            columns: [
                                search.createColumn({
                                    name: 'custrecord_lmry_co_terceros_account',
                                    summary: 'GROUP',
                                    sort: search.Sort.ASC
                                }),
                                search.createColumn({
                                    name: 'custrecord_lmry_co_terceros_debit',
                                    summary: 'SUM'
                                }),
                                search.createColumn({
                                    name: 'custrecord_lmry_co_terceros_credit',
                                    summary: 'SUM'
                                }),
                                search.createColumn({
                                    name: 'custrecord_lmry_co_terceros_entity',
                                    summary: 'GROUP'
                                })
                            ]
                        });
                    }else{
                        var busqueda = search.create({
                            type: 'customrecord_lmry_co_terceros_data',
                            filters: [
                                search.createFilter({
                                    name : 'custrecord_lmry_co_terceros_year',
                                    operator: search.Operator.GREATERTHANOREQUALTO,
                                    values: [firstYear]
                                }),
                                search.createFilter({
                                    name : 'formulanumeric',
                                    formula: 'CASE WHEN {custrecord_lmry_co_terceros_debit} - {custrecord_lmry_co_terceros_credit} <> 0 THEN 1 ELSE 0 END',
                                    operator: search.Operator.EQUALTO,
                                    values: [1]
                                }),
                                search.createFilter({
                                    name : 'custrecord_lmry_co_terceros_year',
                                    operator: search.Operator.LESSTHANOREQUALTO,
                                    values: [lastYear]
                                }),
                                search.createFilter({
                                    name : 'custrecord_lmry_co_terceros_subsi',
                                    operator: search.Operator.IS,
                                    values: [paramSubsidy]
                                }),
                                search.createFilter({
                                    name : 'custrecord_lmry_co_terceros_puc6',
                                    operator: search.Operator.STARTSWITH,
                                    values: [paramPUC]
                                })],
                            columns: [
                                search.createColumn({
                                    name: 'custrecord_lmry_co_terceros_account',
                                    summary: 'GROUP',
                                    sort: search.Sort.ASC
                                }),
                                search.createColumn({
                                    name: 'custrecord_lmry_co_terceros_debit',
                                    summary: 'SUM'
                                }),
                                search.createColumn({
                                    name: 'custrecord_lmry_co_terceros_credit',
                                    summary: 'SUM'
                                }),
                                search.createColumn({
                                    name: 'custrecord_lmry_co_terceros_entity',
                                    summary: 'GROUP'
                                })
                            ]
                        });
                    }
                }else{
                    if(feamultibook || feamultibook == 'T'){
                        var busqueda = search.create({
                            type: 'customrecord_lmry_co_terceros_data',
                            filters: [
                                search.createFilter({
                                    name : 'custrecord_lmry_co_terceros_year',
                                    operator: search.Operator.GREATERTHANOREQUALTO,
                                    values: [firstYear]
                                }),
                                search.createFilter({
                                    name : 'formulanumeric',
                                    formula: 'CASE WHEN {custrecord_lmry_co_terceros_debit} - {custrecord_lmry_co_terceros_credit} <> 0 THEN 1 ELSE 0 END',
                                    operator: search.Operator.EQUALTO,
                                    values: [1]
                                }),
                                search.createFilter({
                                    name : 'custrecord_lmry_co_terceros_year',
                                    operator: search.Operator.LESSTHANOREQUALTO,
                                    values: [lastYear]
                                }),
                                search.createFilter({
                                    name : 'custrecord_lmry_co_terceros_multibook',
                                    operator: search.Operator.IS,
                                    values: [paramMultibook]
                                }),
                                search.createFilter({
                                    name : 'custrecord_lmry_co_terceros_puc6',
                                    operator: search.Operator.STARTSWITH,
                                    values: [paramPUC]
                                })],
                            columns: [
                                search.createColumn({
                                    name: 'custrecord_lmry_co_terceros_account',
                                    summary: 'GROUP',
                                    sort: search.Sort.ASC
                                }),
                                search.createColumn({
                                    name: 'custrecord_lmry_co_terceros_debit',
                                    summary: 'SUM'
                                }),
                                search.createColumn({
                                    name: 'custrecord_lmry_co_terceros_credit',
                                    summary: 'SUM'
                                }),
                                search.createColumn({
                                    name: 'custrecord_lmry_co_terceros_entity',
                                    summary: 'GROUP'
                                })
                            ]
                        });
                    }else{
                        var busqueda = search.create({
                            type: 'customrecord_lmry_co_terceros_data',
                            filters: [
                                search.createFilter({
                                    name : 'custrecord_lmry_co_terceros_year',
                                    operator: search.Operator.GREATERTHANOREQUALTO,
                                    values: [firstYear]
                                }),
                                search.createFilter({
                                    name : 'formulanumeric',
                                    formula: 'CASE WHEN {custrecord_lmry_co_terceros_debit} - {custrecord_lmry_co_terceros_credit} <> 0 THEN 1 ELSE 0 END',
                                    operator: search.Operator.EQUALTO,
                                    values: [1]
                                }),
                                search.createFilter({
                                    name : 'custrecord_lmry_co_terceros_year',
                                    operator: search.Operator.LESSTHANOREQUALTO,
                                    values: [lastYear]
                                }),
                                search.createFilter({
                                    name : 'custrecord_lmry_co_terceros_puc6',
                                    operator: search.Operator.STARTSWITH,
                                    values: [paramPUC]
                                })],
                            columns: [
                                search.createColumn({
                                    name: 'custrecord_lmry_co_terceros_account',
                                    summary: 'GROUP',
                                    sort: search.Sort.ASC
                                }),
                                search.createColumn({
                                    name: 'custrecord_lmry_co_terceros_debit',
                                    summary: 'SUM'
                                }),
                                search.createColumn({
                                    name: 'custrecord_lmry_co_terceros_credit',
                                    summary: 'SUM'
                                }),
                                search.createColumn({
                                    name: 'custrecord_lmry_co_terceros_entity',
                                    summary: 'GROUP'
                                })
                            ]
                        });
                    }
                }
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

                        // 0. Account
                        if (objResult[i].getValue(columns[0])!=null  && objResult[i].getValue(columns[0])!='- None -' && objResult[i].getValue(columns[0])!='NaN' && objResult[i].getValue(columns[0])!='undefined'){
                            arrAuxiliar[0] = objResult[i].getValue(columns[0]);
                        }else{
                            arrAuxiliar[0] = '';
                        }
                        
                        // 1. Debit
                        if (objResult[i].getValue(columns[1])!=null  && objResult[i].getValue(columns[1])!='- None -' && objResult[i].getValue(columns[1])!='NaN' && objResult[i].getValue(columns[1])!='undefined'){
                            arrAuxiliar[1] = objResult[i].getValue(columns[1]);
                        }else{
                            arrAuxiliar[1] = '';
                        }
                        
                        // 2. Credit
                        if (objResult[i].getValue(columns[2])!=null  && objResult[i].getValue(columns[2])!='- None -' && objResult[i].getValue(columns[2])!='NaN' && objResult[i].getValue(columns[2])!='undefined'){
                            arrAuxiliar[2] = objResult[i].getValue(columns[2]);
                        }else{
                            arrAuxiliar[2] = '';
                        }
                        
                        // 3. Entity
                        if(objResult[i].getValue(columns[3]) != null && objResult[i].getValue(columns[3]) != '- None -' && objResult[i].getValue(columns[3])!='NaN' && objResult[i].getValue(columns[3])!='undefined'){
                            arrAuxiliar[3] = objResult[i].getValue(columns[3]);
                        }else{
                            arrAuxiliar[3] = '';
                        }

                        if(paramEntity != null){
                            if(arrAuxiliar[3] != ''){
                                var entityJSON = JSON.parse(arrAuxiliar[3]);

                                if(entityJSON.internalid == paramEntity){
                                    ArrReturn[cont] = arrAuxiliar;
                                    cont++;
                                }
                            }
                        }else{
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

        function ObtenerPeriodosDelAño(ArrAllPeriods){
            var ArrReturn = new Array();

            for(var i = 0; i < ArrAllPeriods.length; i++){
                var tempYear = format.parse({
                    value: ArrAllPeriods[i][1],
                    type: format.Type.DATE
                }).getFullYear();

                var tempMonth = format.parse({
                    value: ArrAllPeriods[i][1],
                    type: format.Type.DATE
                }).getMonth();

                if(periodIniIsAdjust){
                    if(tempYear == periodYearIni && tempMonth <= periodMonthIni && paramPeriod != ArrAllPeriods[i][0]){
                        var arr = new Array();

                        arr[0] = ArrAllPeriods[i][0];

                        arr[1] = ArrAllPeriods[i][1];

                        ArrReturn.push(arr);
                    }
                }else{
                    if(tempYear == periodYearIni && tempMonth < periodMonthIni ){
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

        function OrdenarPeriodosPorMes(arrTemporal){
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

                    if (Number(a) > Number(b)){
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

        function ObtenerPeriodos(){
            var intDMinReg = 0;
            var intDMaxReg = 1000;

            var DbolStop = false;
            var ArrReturn = new Array();
            var cont = 0;

            var busqueda = search.create({
                type: search.Type.ACCOUNTING_PERIOD,
                filters: [
                    // search.createFilter({
                    //     name : 'isadjust',
                    //     operator: search.Operator.IS,
                    //     values: ['F']
                    // }),
                    search.createFilter({
                        name : 'isquarter',
                        operator: search.Operator.IS,
                        values: ['F']
                    }),
                    search.createFilter({
                        name : 'isinactive',
                        operator: search.Operator.IS,
                        values: ['F']
                    }),
                    search.createFilter({
                        name : 'isyear',
                        operator: search.Operator.IS,
                        values: ['F']
                    })],
                columns: ['internalid','startdate']
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
                        if (objResult[i].getValue(columns[0])!=null  && objResult[i].getValue(columns[0])!='- None -' && objResult[i].getValue(columns[0])!='NaN' && objResult[i].getValue(columns[0])!='undefined'){
                            arrAuxiliar[0] = objResult[i].getValue(columns[0]);
                        }else{
                            arrAuxiliar[0] = '';
                        }
                        
                        // 1. Start Date
                        if (objResult[i].getValue(columns[1])!=null  && objResult[i].getValue(columns[1])!='- None -' && objResult[i].getValue(columns[1])!='NaN' && objResult[i].getValue(columns[1])!='undefined'){
                            arrAuxiliar[1] = objResult[i].getValue(columns[1]);
                        }else{
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

        function OrdenarAños(arrTemporal){
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

        function ObtenerAñosProcesados(){
            var intDMinReg = 0;
            var intDMaxReg = 1000;
            var DbolStop = false;

            var ArrReturn = new Array();
            var cont = 0;

            if(feamultibook){
                if(featuresubs){
                    var busqueda = search.create({
                        type: 'customrecord_lmry_co_terceros_procesados',
                        filters: [
                            ['isinactive', 'is', 'F'],
                            'AND',
                            ['custrecord_lmry_co_multibook_procesado', 'is', paramMultibook], 
                            'AND',
                            ['custrecord_lmry_co_subsi_procesado', 'is', paramSubsidy]
                        ],
                        columns: ['internalid','custrecord_lmry_co_year_procesado', 'custrecord_lmry_co_multibook_procesado', 'custrecord_lmry_co_subsi_procesado', 'custrecord_lmry_co_puc_procesado']
                    });
                }else{
                    var busqueda = search.create({
                        type: 'customrecord_lmry_co_terceros_procesados',
                        filters: [
                            ['isinactive', 'is', 'F'],
                            'AND',
                            ['custrecord_lmry_co_multibook_procesado', 'is', paramMultibook]],
                        columns: ['internalid','custrecord_lmry_co_year_procesado', 'custrecord_lmry_co_multibook_procesado', 'custrecord_lmry_co_subsi_procesado', 'custrecord_lmry_co_puc_procesado']
                    });
                }
            }else{
                if(featuresubs){
                    var busqueda = search.create({
                        type: 'customrecord_lmry_co_terceros_procesados',
                        filters: [
                            ['isinactive', 'is', 'F'],
                            'AND',
                            ['custrecord_lmry_co_subsi_procesado', 'is', paramSubsidy]
                    ],
                        columns: ['internalid','custrecord_lmry_co_year_procesado', 'custrecord_lmry_co_multibook_procesado', 'custrecord_lmry_co_subsi_procesado', 'custrecord_lmry_co_puc_procesado']
                    });
                }else{
                    var busqueda = search.create({
                        type: 'customrecord_lmry_co_terceros_procesados',
                        filters: [
                            ['isinactive', 'is', 'F']
                        ],
                        columns: ['internalid','custrecord_lmry_co_year_procesado', 'custrecord_lmry_co_multibook_procesado', 'custrecord_lmry_co_subsi_procesado', 'custrecord_lmry_co_puc_procesado']
                    });
                }
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
                        if (objResult[i].getValue(columns[0])!=null  && objResult[i].getValue(columns[0])!='- None -' && objResult[i].getValue(columns[0])!='NaN' && objResult[i].getValue(columns[0])!='undefined'){
                            arrAuxiliar[0] = objResult[i].getValue(columns[0]);
                        }else{
                            arrAuxiliar[0] = '';
                        }
                        
                        // 1. Año
                        if (objResult[i].getValue(columns[1])!=null  && objResult[i].getValue(columns[1])!='- None -' && objResult[i].getValue(columns[1])!='NaN' && objResult[i].getValue(columns[1])!='undefined'){
                            arrAuxiliar[1] = objResult[i].getValue(columns[1]);
                        }else{
                            arrAuxiliar[1] = '';
                        }
                        
                        // 2. Multibook
                        if (objResult[i].getValue(columns[2])!=null  && objResult[i].getValue(columns[2])!='- None -' && objResult[i].getValue(columns[2])!='NaN' && objResult[i].getValue(columns[2])!='undefined'){
                            arrAuxiliar[2] = objResult[i].getValue(columns[2]);
                        }else{
                            arrAuxiliar[2] = '';
                        }
                        
                        // 3. Subsidiaria
                        if (objResult[i].getValue(columns[3])!=null  && objResult[i].getValue(columns[3])!='- None -' && objResult[i].getValue(columns[3])!='NaN' && objResult[i].getValue(columns[3])!='undefined'){
                            arrAuxiliar[3] = objResult[i].getValue(columns[3]);
                        }else{
                            arrAuxiliar[3] = '';
                        }
                        
                        // 4. PUC
                        if (objResult[i].getValue(columns[4])!=null  && objResult[i].getValue(columns[4])!='- None -' && objResult[i].getValue(columns[4])!='NaN' && objResult[i].getValue(columns[4])!='undefined'){
                            arrAuxiliar[4] = objResult[i].getValue(columns[4]);
                        }else{
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

        function ObtenerData(periodYearIniID, type, isSpecific, isForRecord, paramPeriodFin){
            var intDMinReg = 0;
            var intDMaxReg = 1000;

            var DbolStop = false;
            var ArrReturn = new Array();
            var cont = 0;

            var savedsearch = search.load({
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

            if(!isForRecord){
                if (paramEntity != null) {
                    //log.error('filtrooooo');
                    
                    var formulaEntity = 'CASE WHEN NVL({custcol_lmry_exp_rep_vendor_colum.internalid}, NVL({entity.id}, ' +
                                        'NVL({customer.internalid}, NVL({vendor.internalid}, NVL({vendorline.internalid},' + 
                                        ' {employee.internalid}))))) = ' + paramEntity + ' THEN 1 ELSE 0 END';

                    //log.error('formulaEntity', formulaEntity);

                    var entityFilter = search.createFilter({
                        name: 'formulatext',
                        formula: formulaEntity,
                        operator: search.Operator.IS,
                        values: [1]
                    });
                    
                    savedsearch.filters.push(entityFilter);
                }
            }

            if(feamultibook){
                var amountFilter = search.createFilter({
                    name: 'formulanumeric',
                    operator: search.Operator.EQUALTO,
                    formula: "CASE WHEN NVL({accountingtransaction.debitamount},0) - NVL({accountingtransaction.creditamount},0) <> 0 THEN 1 ELSE 0 END",
                    values: [1]
                });
                savedsearch.filters.push(amountFilter);
            }else{
                var amountFilter = search.createFilter({
                    name: 'formulanumeric',
                    operator: search.Operator.EQUALTO,
                    formula: "CASE WHEN NVL({debitamount},0) - NVL({creditamount},0) <> 0 THEN 1 ELSE 0 END",
                    values: [1]
                });
                savedsearch.filters.push(amountFilter);
            }

            if(type){
                // Movimientos
                if(paramPeriodFin != null){
                    var periodoInicioDate = format.format({
                        value: periodstartdateIni,
                        type: format.Type.DATE
                    });

                    var periodoFinDate = format.format({
                        value: periodstartdateFin,
                        type: format.Type.DATE
                    });

                    var periodFilterIni = search.createFilter({
                        name: 'startdate',
                        join: 'accountingperiod',
                        operator: search.Operator.ONORAFTER,
                        values: [periodoInicioDate]
                    });
                    savedsearch.filters.push(periodFilterIni);

                    var periodFilterFin = search.createFilter({
                        name: 'startdate',
                        join: 'accountingperiod',
                        operator: search.Operator.ONORBEFORE,
                        values: [periodoFinDate]
                    });
                    savedsearch.filters.push(periodFilterFin);
                }else{
                    var periodFilter = search.createFilter({
                        name: 'postingperiod',
                        operator: search.Operator.IS,
                        values: [periodYearIniID]
                    });
                    savedsearch.filters.push(periodFilter);
                }
            }else{
                // Saldo Anterior
                var arrTemp = new Array();

                for(var i = 0; i < periodYearIniID.length; i++){
                    arrTemp[i] = periodYearIniID[i][0];
                }

                var periodFilter = search.createFilter({
                    name: 'postingperiod',
                    operator: search.Operator.ANYOF,
                    values: [arrTemp]
                });
                savedsearch.filters.push(periodFilter);
            }

            if (feamultibook) {
                if(isSpecific){
                    var specificFilter = search.createFilter({
                        name: 'bookspecifictransaction',
                        operator: search.Operator.IS,
                        values: ['T']
                    });

                    savedsearch.filters.push(specificFilter);
                }else{
                    var specificFilter = search.createFilter({
                        name: 'bookspecifictransaction',
                        operator: search.Operator.IS,
                        values: ['F']
                    });

                    savedsearch.filters.push(specificFilter);

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
                //columna 7
                // var pucColumn = search.createColumn({
                //     name: 'custrecord_lmry_co_puc_d6_id',
                //     join: 'account',
                //     summary: 'GROUP'
                // });
                // savedsearch.columns.push(pucColumn);
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
                            if (objResult[i].getValue(columns[6])!=null  && objResult[i].getValue(columns[6])!='- None -' && objResult[i].getValue(columns[6])!='NaN' && objResult[i].getValue(columns[6])!='undefined'){
                                arr[0] = objResult[i].getValue(columns[6]);
                            }else{
                                arr[0] = '';
                            }
                            
                            // 1. Debit
                            if (objResult[i].getValue(columns[4])!=null  && objResult[i].getValue(columns[4])!='- None -' && objResult[i].getValue(columns[4])!='NaN' && objResult[i].getValue(columns[4])!='undefined'){
                                arr[1] = objResult[i].getValue(columns[4]);
                            }else{
                                arr[1] = '';
                            }
                            
                            // 2. Credit
                            if (objResult[i].getValue(columns[5])!=null  && objResult[i].getValue(columns[5])!='- None -' && objResult[i].getValue(columns[5])!='NaN' && objResult[i].getValue(columns[5])!='undefined'){
                                arr[2] = objResult[i].getValue(columns[5]);
                            }else{
                                arr[2] = '';
                            }
                            
                            // 3. Entity
                            if(objResult[i].getValue(columns[3]) != null && objResult[i].getValue(columns[3]) != '' && objResult[i].getValue(columns[3]) != '- None -'){
                                arr[3] = objResult[i].getValue(columns[3]);
                            }else{
                                arr[3] = '';
                            }
                            //arr[4] = objResult[i].getValue(columns[7]);
                        } else {
                            // 0. Account
                            if (objResult[i].getValue(columns[0])!=null  && objResult[i].getValue(columns[0])!='- None -' && objResult[i].getValue(columns[0])!='NaN' && objResult[i].getValue(columns[0])!='undefined'){
                                arr[0] = objResult[i].getValue(columns[0]);
                            }else{
                                arr[0] = '';
                            }
                            
                            // 1. Debit
                            if (objResult[i].getValue(columns[1])!=null  && objResult[i].getValue(columns[1])!='- None -' && objResult[i].getValue(columns[1])!='NaN' && objResult[i].getValue(columns[1])!='undefined'){
                                arr[1] = objResult[i].getValue(columns[1]);
                            }else{
                                arr[1] = '';
                            }
                            
                            // 2. Credit
                            if (objResult[i].getValue(columns[2])!=null  && objResult[i].getValue(columns[2])!='- None -' && objResult[i].getValue(columns[2])!='NaN' && objResult[i].getValue(columns[2])!='undefined'){
                                arr[2] = objResult[i].getValue(columns[2]);
                            }else{
                                arr[2] = '';
                            }
                            
                            // 3. Entity
                            if(objResult[i].getValue(columns[3]) != null && objResult[i].getValue(columns[3]) != '' && objResult[i].getValue(columns[3]) != '- None -'){
                                arr[3] = objResult[i].getValue(columns[3]);
                            }else{
                                arr[3] = '';
                            }
                        }

                        if(paramEntity != null){
                            if(!isForRecord){
                                if(paramEntity == arr[3]){
                                    ArrReturn[cont] = arr;
                                    cont++;
                                }
                            }else{
                                ArrReturn[cont] = arr;
                                cont++;
                            }
                        }else{
                            ArrReturn[cont] = arr;
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

        function ParametrosYFeatures(){
            paramEntity = objContext.getParameter({
                    name: 'custscript_lmry_terceros_mprdc_entity'
            });

            paramMultibook = objContext.getParameter({
                    name: 'custscript_lmry_terceros_mprdc_multi'
            });

            paramRecordID = objContext.getParameter({
                    name: 'custscript_lmry_terceros_mprdc_record'
            });

            paramSubsidy = objContext.getParameter({
                    name: 'custscript_lmry_terceros_mprdc_subsi'
            });

            paramPeriod = objContext.getParameter({
                    name: 'custscript_lmry_terceros_mprdc_period'
            });

            paramPUC = objContext.getParameter({
                    name: 'custscript_lmry_terceros_mprdc_lastpuc'
            });

            paramPeriodFin = objContext.getParameter({
                    name: 'custscript_lmry_terceros_mprdc_periodFin'
            });

            if(paramPUC == null){
                paramPUC = 1;
            }
          //log.error('parametros:',' entity -'+paramEntity+' Multibook -'+paramMultibook+' recorID -'+paramRecordID+' Subsi -'+paramSubsidy+' periodo -'+paramPeriod+' PUC -'+paramPUC+' periodFIn -'+paramPeriodFin);


            var period_temp = search.lookupFields({
                type: search.Type.ACCOUNTING_PERIOD,
                id: paramPeriod,
                columns: ['startdate', 'isadjust']
            });

            periodstartdateIni = period_temp.startdate;

            periodIniIsAdjust = period_temp.isadjust;

            periodYearIni = format.parse({
                value: periodstartdateIni,
                type: format.Type.DATE
            }).getFullYear();

            periodMonthIni = format.parse({
                value: periodstartdateIni,
                type: format.Type.DATE
            }).getMonth();

            if(paramPeriodFin != null && paramPeriodFin != ''){
                var period_temp_2 = search.lookupFields({
                    type: search.Type.ACCOUNTING_PERIOD,
                    id: paramPeriodFin,
                    columns: ['startdate']
                });

                periodstartdateFin = period_temp_2.startdate;

                periodYearFin = format.parse({
                    value: periodstartdateFin,
                    type: format.Type.DATE
                }).getFullYear();

                periodMonthFin = format.parse({
                    value: periodstartdateFin,
                    type: format.Type.DATE
                }).getMonth();
            }

            featuresubs = runtime.isFeatureInEffect({
                feature: "SUBSIDIARIES"
            });

            feamultibook = runtime.isFeatureInEffect({
                feature: "MULTIBOOK"
            });

            featurejobs = runtime.isFeatureInEffect({
                feature: "JOBS"
            });
        }

        function ObtenerAñosFiscales(){
            var intDMinReg = 0;
            var intDMaxReg = 1000;
            var DbolStop = false;

            var ArrReturn = new Array();
            var cont = 0;

            var busqueda = search.create({
                type: search.Type.ACCOUNTING_PERIOD,
                filters: [
                    search.createFilter({
                        name : 'isyear',
                        operator: search.Operator.IS,
                        values: ['T']
                    }),
                    search.createFilter({
                        name : 'isinactive',
                        operator: search.Operator.IS,
                        values: ['F']
                    })
                ],
                columns: ['internalid','startdate']
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
                        if (objResult[i].getValue(columns[0])!=null  && objResult[i].getValue(columns[0])!='- None -' && objResult[i].getValue(columns[0])!='NaN' && objResult[i].getValue(columns[0])!='undefined'){
                            arrAuxiliar[0] = objResult[i].getValue(columns[0]);
                        }else{
                            arrAuxiliar[0] = '';
                        }

                        // 1. Start Date
                        if (objResult[i].getValue(columns[1])!=null  && objResult[i].getValue(columns[1])!='- None -' && objResult[i].getValue(columns[1])!='NaN' && objResult[i].getValue(columns[1])!='undefined'){
                            arrAuxiliar[1] = objResult[i].getValue(columns[1]);
                        }else{
                            arrAuxiliar[1] = '';
                        }
                        
                        var startDateYearTemp = format.parse({
                            value: arrAuxiliar[1],
                            type: format.Type.DATE
                        }).getFullYear();

                        arrAuxiliar[1] = startDateYearTemp;

                        if(startDateYearTemp < periodYearIni){
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