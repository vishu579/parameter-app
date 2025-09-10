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
        toggleSortOrder() {
            this.sortAsc = !this.sortAsc;
            if (this.selectedSortKey) {
                this.sortBy(this.selectedSortKey, false);
            }
        }
    },
    mounted() {
        this.fetchData();
    }
});