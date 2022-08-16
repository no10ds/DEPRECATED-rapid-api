const goToNextStep = () => {
    const subjectId = document.getElementById("select_subject").value
    window.location.href = `subject/${subjectId}/modify`
}

const toggleModifyButton = (event) => {
    document.getElementById('button-modify').disabled = event.target.value === ''
}

document.getElementById('select_subject').addEventListener('change', toggleModifyButton)
