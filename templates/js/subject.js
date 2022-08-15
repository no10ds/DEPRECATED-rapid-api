const goToNextStep = () => {
    const subjectId = document.getElementById("select_subject").value
    window.location.href = `subject/${subjectId}/modify`
}
