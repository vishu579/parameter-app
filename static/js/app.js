new Vue({
    el: '#app',
    delimiters: ['${', '}'],
    data: {
        rawData: [],
        filteredData: [],
        loading: false,
        error: null,
        searchQuery: '',
        selectedSortKey: '',
        sortAsc: true,
        sortableKeys: ['id', 'param_name', 'param_theme', 'param_gen_frequency', 'aggregation_period', 'param_displayname'],
        key_id: null,
        paramRawData: [],
        paramFilteredData: [],
        paramLoading: false,
        paramError: null,
        paramSelectedSortKey: '',
        paramSortAsc: true,
        paramSortableKeys: ['param_id', 'param_name', 'geoentity_source_id', 'geoentity_name', 'param_theme'],
        selectedCategoricalData: null,
        selectedCategoricalFn: null,
        folderPathMain: '',
        folderPathSub: '',
        stats: [],
        replaceDaysStart: null,
        replaceDaysEnd: null,
        folderPathVerifyResult: '',
        folderPathVerifying: false,
        param_name: '',
        entities: [
            {
                name: '',
                source_id: '',
                regionPrefixFilters: [''],
                params: ''
            }
        ],
        sourceOptions: [],
        configs: [],
        configLoading: false,
        configError: null,
        runningStatus: {},
        pollingIntervalId: null,
        isPolling: false,
    },
    computed: {
        keyLabels() {
            return {
                id: 'ID',
                param_name: 'Param Name',
                param_theme: 'Param Theme',
                param_gen_frequency: 'Param Gen Frequency',
                aggregation_period: 'Aggregation Period',
                param_displayname: 'Param Display Name',
            };
        },
        paramKeyLabels() {
            return {
                param_id: 'Param Id',
                param_name: 'Param Name',
                geoentity_source_id: 'Geoentity Source ID',
                param_theme: 'Param Theme',
            };
        }
    },
    methods: {
        fetchData() {
            this.loading = true;
            this.error = null;
            fetch('http://192.168.2.202:5001/parameters')
                .then(res => res.json())
                .then(data => {
                    this.rawData = data.data;
                    this.filteredData = this.rawData.slice();
                })
                .catch(() => {
                    this.error = 'Failed to load data';
                })
                .finally(() => {
                    this.loading = false;
                });
        },
        filterData() {
            const q = this.searchQuery.toLowerCase().trim();
            this.filteredData = this.rawData.filter(item => {
                const matchesSearch =
                    !q ||
                    (item.id && item.id.toString().includes(q)) ||
                    (item.param_name && item.param_name.toLowerCase().includes(q)) ||
                    (item.param_theme && item.param_theme.toLowerCase().includes(q)) ||
                    (item.param_gen_frequency && item.param_gen_frequency.toLowerCase().includes(q)) ||
                    (item.aggregation_period && item.aggregation_period.toLowerCase().includes(q)) ||
                    (item.param_displayname && item.param_displayname.toLowerCase().includes(q));
                return matchesSearch;
            });

            if (this.selectedSortKey) {
                this.sortBy(this.selectedSortKey, false);
            }
        },
        sortBy(key, toggle = true) {
            if (toggle) {
                if (this.selectedSortKey === key) {
                    this.sortAsc = !this.sortAsc;
                } else {
                    this.selectedSortKey = key;
                    this.sortAsc = true;
                }
            }

            this.filteredData.sort((a, b) => {
                const valA = a[key] != null ? a[key] : '';
                const valB = b[key] != null ? b[key] : '';
                if (typeof valA === 'number' && typeof valB === 'number') {
                    return this.sortAsc ? valA - valB : valB - valA;
                }
                return this.sortAsc
                    ? String(valA).localeCompare(String(valB))
                    : String(valB).localeCompare(String(valA));
            });
        },
        handleSortChange() {
            if (this.selectedSortKey) {
                this.sortBy(this.selectedSortKey, false);
            }
        },
        redirectToParam(id) {
            window.location.href = `/list/key=${id}`;
        },
        fetchParamData() {
            this.paramLoading = true;
            this.paramError = null;
            fetch(`http://192.168.2.202:5001/params-source-ids/${this.key_id}`)
                .then(res => res.json())
                .then(data => {
                    this.paramRawData = data.data;
                    this.paramFilteredData = this.paramRawData.slice();

                    this.paramSelectedSortKey = 'geoentity_source_id';
                    this.paramSortAsc = true;
                    this.paramSortBy('geoentity_source_id', false);
                })
                .catch(() => {
                    this.paramError = 'Failed to load parameter data';
                })
                .finally(() => {
                    this.paramLoading = false;
                });
        },
        paramSortBy(key, toggle = true) {
            if (toggle) {
                if (this.paramSelectedSortKey === key) {
                    this.paramSortAsc = !this.paramSortAsc;
                } else {
                    this.paramSelectedSortKey = key;
                    this.paramSortAsc = true;
                }
            }

            this.paramFilteredData.sort((a, b) => {
                const valA = a[key] != null ? a[key] : '';
                const valB = b[key] != null ? b[key] : '';
                if (typeof valA === 'number' && typeof valB === 'number') {
                    return this.paramSortAsc ? valA - valB : valB - valA;
                }
                return this.paramSortAsc
                    ? String(valA).localeCompare(String(valB))
                    : String(valB).localeCompare(String(valA));
            });
        },
        goBack() {
            window.location.href = '/';
        },
        goToRegister() {
            if (this.paramFilteredData.length === 0) {
                alert("No data available.");
                return;
            }

            const paramName = this.paramFilteredData[0].param_name;
            const encodedName = encodeURIComponent(paramName);

            if (!this.key_id) {
                alert("Key ID not set.");
                return;
            }

            window.location.href = `/register/${encodedName}/key=${this.key_id}`;
        },
        async verifyFolderPath() {
            console.log('Verify button clicked');
            const mainPath = this.folderPathMain;
            const subPath = this.folderPathSub.trim();

            if (!mainPath) {
                alert('Please select a main folder path first.');
                return;
            }

            const fullPath = subPath ? `${mainPath}${subPath}` : mainPath;

            console.log(`Verifying path: ${fullPath}`);


            this.folderPathVerifyResult = 'Verifying...';
            this.folderPathVerifying = true;

            try {
                const response = await fetch('/verify-path', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                        // Add CSRF token header if you need it here
                    },
                    body: JSON.stringify({ path: fullPath })
                });

                console.log("Fetch response received");


                if (!response.ok) {
                    this.folderPathVerifyResult = 'Failed to verify path (server error).';
                    this.folderPathVerifying = false;
                    return;
                }

                const data = await response.json();

                console.log("Response data:", data);

                if (data.exists) {
                    this.folderPathVerifyResult = 'Path exists ✅';
                    // alert('Path verified successfully!');
                } else {
                    this.folderPathVerifyResult = 'Path does not exist ❌';
                    // alert('Path does not exist.');
                }
            } catch (error) {
                this.folderPathVerifyResult = 'Error verifying path.';
                console.error(error);
            } finally {
                this.folderPathVerifying = false;
            }
        },
        addEntity() {
            this.entities.push({
                name: '',
                source_id: '',
                regionPrefixFilters: [''],
                params: this.param_name,
                isNew: true  // Mark this entity as new
            });
        },
        removeEntity(index) {
            if (this.entities.length > 1) {
                this.entities.splice(index, 1);
            }
        },
        addRegionFilter(entityIndex) {
            this.entities[entityIndex].regionPrefixFilters.push('');
        },
        removeRegionFilter(entityIndex, filterIndex) {
            if (this.entities[entityIndex].regionPrefixFilters.length > 1) {
                this.entities[entityIndex].regionPrefixFilters.splice(filterIndex, 1);
            }
        },
        fetchSourceOptions() {
            fetch('https://vedas.sac.gov.in/geoentity-services/api/geoentity-sources/')
                .then(response => response.json())
                .then(data => {
                    // Sort data by 'id' ascending (or 'name' if you prefer)
                    const sortedData = data.data.sort((a, b) => {
                        // Sort by id (numeric)
                        return a.id - b.id;
                    });

                    this.sourceOptions = sortedData;
                })
                .catch(err => {
                    console.error('Failed to load source options:', err);
                });
        },
        goToExistingConfigs() {
            window.location.href = '/existing-configs';
        },
        fetchConfigs() {
            this.configLoading = true;
            fetch('/api/configs')
                .then(res => res.json())
                .then(data => {
                    if (data.error) {
                        this.configError = data.error;
                        this.configs = [];
                    } else {
                        this.configs = data.data || [];
                        this.configError = null;
                    }
                })
                .catch(err => {
                    this.configError = 'Failed to load configs: ' + err.message;
                })
                .finally(() => {
                    this.configLoading = false;
                });
        },
        getParamIdByFileName(fileName) {
            const param = this.rawData.find(item => item.param_name === fileName);
            return param ? param.id : '';
        },
        loadConfigData(config) {
            if (!config || !config.config) return;

            const paramTemplate = config.config.param_template || {};
            const paramKey = this.param_name;
            if (paramTemplate[paramKey] && paramTemplate[paramKey].length > 0) {
                const paramConfig = paramTemplate[paramKey][0];

                // Folder path
                if (paramConfig.folder_path) {
                    if (paramConfig.folder_path.startsWith("/home/isro/68_data/")) {
                        this.folderPathMain = "/home/isro/68_data/";
                        this.folderPathSub = paramConfig.folder_path.replace("/home/isro/68_data/", "");
                    } else if (paramConfig.folder_path.startsWith("/data/VEDAS_Projects_Datasets/")) {
                        this.folderPathMain = "/data/VEDAS_Projects_Datasets/";
                        this.folderPathSub = paramConfig.folder_path.replace("/data/VEDAS_Projects_Datasets/", "");
                    } else if (paramConfig.folder_path.startsWith("/home/RAW_DATA/RAW_DATA/")) {
                        this.folderPathMain = "/home/RAW_DATA/RAW_DATA/";
                        this.folderPathSub = paramConfig.folder_path.replace("/home/RAW_DATA/RAW_DATA/", "");
                    } else {
                        this.folderPathMain = '';
                        this.folderPathSub = paramConfig.folder_path;
                    }
                }

                // categorical
                this.selectedCategoricalData = paramConfig.categorical_data ? "true" : "false";
                this.selectedCategoricalFn = paramConfig.categorical_fn || null;

                // stats
                this.stats = paramConfig.stats || [];

                // update flag
                document.querySelectorAll("input[name='update_flag']").forEach(radio => {
                    if (radio.value === (paramConfig.update_flag ? "true" : "false")) {
                        radio.checked = true;
                    }
                });

                if (Array.isArray(paramConfig.replace_days_range) && paramConfig.replace_days_range.length === 2) {
                    this.replaceDaysStart = paramConfig.replace_days_range[0];
                    this.replaceDaysEnd = paramConfig.replace_days_range[1];
                } else {
                    this.replaceDaysStart = null;
                    this.replaceDaysEnd = null;
                }
            }

            // Entities
            const entityMapping = config.config.mapping.entity_mapping || {};
            this.entities = Object.keys(entityMapping).map(entityName => {
                const entity = entityMapping[entityName];
                return {
                    name: entityName,
                    source_id: entity.source_id || '',
                    regionPrefixFilters: entity.region_prefix_filter || [''],
                    params: entity.params || this.param_name,
                    isNew: false  // Mark preloaded entities as NOT new
                };
            });

            if (this.entities.length === 0) {
                this.entities = [
                    {
                        name: '',
                        source_id: '',
                        regionPrefixFilters: [''],
                        params: this.param_name,
                        isNew: true  // New entity to be editable
                    }
                ];
            }
        },
        runProcess(config) {
            if (this.runningStatus[config.file_name] === 'running') return;

            fetch('/api/run_process', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ config_file: config.file_name })
            })
                .then(res => res.json())
                .then(data => {
                    if (data.status === 'started') {
                        alert(`Job has started for this id: ${config.file_name}`);
                        this.$set(this.runningStatus, config.file_name, 'running');

                        // Start polling if not already started
                        if (!this.isPolling) {
                            this.startPolling();
                        }
                    } else if (data.status === 'running') {
                        alert(data.message);
                    } else {
                        alert('Failed to start job');
                    }
                })
                .catch(() => alert('Error starting job'));
        },
        pollStatus() {
            this.configs.forEach(config => {
                if (this.runningStatus[config.file_name] === 'running') {
                    fetch(`/api/status/${encodeURIComponent(config.file_name)}`)
                        .then(res => res.json())
                        .then(data => {
                            const oldStatus = this.runningStatus[config.file_name];
                            const newStatus = data.status;

                            if (oldStatus !== newStatus) {
                                if (newStatus === 'completed' || newStatus === 'failed') {
                                    alert(`Job has been ${newStatus} for this id: ${config.file_name}`);
                                    this.$set(this.runningStatus, config.file_name, newStatus);
                                }
                            }
                        })
                }
            })
        },
        startPolling() {
            this.isPolling = true;
            this.pollingIntervalId = setInterval(() => {
                this.pollStatus();

                // Stop polling if no job is running
                const anyRunning = Object.values(this.runningStatus).some(status => status === 'running');
                if (!anyRunning) {
                    clearInterval(this.pollingIntervalId);
                    this.isPolling = false;
                }
            }, 3000);
        },
    },
    mounted() {
        this.fetchData();
        if (typeof key_id !== 'undefined') {
            this.key_id = key_id;
        } // Assign the variable key_id, make sure it is defined in the scope
        if (typeof param_name !== 'undefined' && param_name !== null) {
            this.param_name = param_name;
        }

        if (typeof initialConfig !== 'undefined' && initialConfig) {
            this.loadConfigData(initialConfig);
        }

        // Set params field for initial entity
        if (this.entities.length > 0) {
            this.entities[0].params = this.param_name;
        }

        this.fetchParamData();
        this.fetchSourceOptions();
        this.fetchConfigs();
    }
});