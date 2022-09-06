const goToNextStep = () => {
    const dataset = document.getElementById("select_dataset").value;
    const version = document.getElementById('select_dataset_version').value;
    window.location.href = `download/${dataset}?version=${version}`;
}

const toggleModifyButton = (event) => {
    const isEmptySelection = event.target.value === '';
    const selectDatasetVersion = document.getElementById('select_dataset_version');
    document.getElementById('button-modify').disabled = isEmptySelection;
    document.getElementById('version-form-select').hidden = isEmptySelection;
    selectDatasetVersion.disabled = isEmptySelection;
    const datasets = document.getElementById('select_dataset');
    const selected_dataset = datasets.options[datasets.selectedIndex];
    const versions = selected_dataset.getAttribute("data-versions");
    let html = '';
    for(let version = versions; version > 0; version--){
        html = html + '<option value="'+version+'">' + version + '</option>';
    }
    selectDatasetVersion.innerHTML = html;

}

document.getElementById('select_dataset').addEventListener('change', toggleModifyButton);
