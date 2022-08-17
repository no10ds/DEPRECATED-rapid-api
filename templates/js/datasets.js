const goToNextStep = () => {
    const dataset = document.getElementById("select_dataset").value
    window.location.href = `download/${dataset}`
}

const toggleModifyButton = (event) => {
    document.getElementById('button-modify').disabled = event.target.value === ''
}

document.getElementById('select_dataset').addEventListener('change', toggleModifyButton)
