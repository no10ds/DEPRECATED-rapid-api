function addListValueToQuery(queryBody, queryField, elementId) {
    const valueToAdd = document.getElementById(elementId).value;
    if (valueToAdd) {
        queryBody[queryField] = valueToAdd.split(",");
   }
}

function addStringValueToQuery(queryBody, queryField, elementId) {
    const valueToAdd = document.getElementById(elementId).value;
    if (valueToAdd) {
        queryBody[queryField] = valueToAdd;
   }
}

function createQuery() {
   const queryBody = {};
   addListValueToQuery(queryBody, "select_columns", "selectColumns");
   addStringValueToQuery(queryBody, "filter", "filter");
   addListValueToQuery(queryBody, "group_by_columns", "groupByColumns");
   addStringValueToQuery(queryBody, "aggregation_conditions", "aggregationConditions");
   addStringValueToQuery(queryBody, "limit", "rowLimit");

   return queryBody
}

const downloadDataset = (domain, dataset) => {
    const selected_format = get_selected_value("select_format");

    const accept_header = selected_format == "json" ? "application/json" : "text/csv"

    fetch(`/datasets/${domain}/${dataset}/query`, {
        method: "POST",
        headers: new Headers({
            "Accept": accept_header,
            "Content-Type": "application/json",
        }),
        body: JSON.stringify(createQuery()),
    }).then(response => {
        if(response.ok) {
            return response.blob().then(blob => {
                // disable submit button till the dataset downloads
                toggleSwitchDisable('download-dataset', true)

                downloadFile(blob, domain, dataset, selected_format)
                // enable submit button back
                toggleSwitchDisable('download-dataset', false)
                }
            )
        } else {
            return response.json().then(result => showErrorMessage(result["details"]))
        }}).catch(_ => {
        showErrorMessage('Something went wrong. Please contact your system administrator.')
        // enable submit button back
        toggleSwitchDisable('download-dataset', false)
    });
};

function toggleSwitchDisable(elementId, isDisabled) {
    elementToToggle = document.getElementById(elementId);
    elementToToggle.disabled = isDisabled
}

function downloadFile(blob, domain, dataset, selected_format) {
                const url = window.URL.createObjectURL(blob);
                const a = document.createElement('a');
                a.style.display = 'none';
                a.href = url;
                a.download = `${domain}_${dataset}.${selected_format}`;
                document.body.appendChild(a);
                a.click();
                window.URL.revokeObjectURL(url);
}

setupNumericValuesEvents('rowLimit')
