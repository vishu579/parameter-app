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
            window.history.back();
        }
    },
    mounted() {
        this.fetchData();
        this.key_id = key_id; // Assign the variable key_id, make sure it is defined in the scope
        this.fetchParamData();
    }
});