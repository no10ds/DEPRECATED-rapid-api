const onInput = (event) => {
    let modifySubjectButton = document.getElementById("button-modify");
    modifySubjectButton.disabled = event.target.value.length < 1
}

document.getElementById("subject-id-input").addEventListener('input', onInput)


const goToNextStep = () => {
    const subjectId = document.getElementById("subject-id-input").value
    window.location.href = `subject/${subjectId}/modify`
}
