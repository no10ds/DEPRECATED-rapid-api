const onInput = (event) => {
    let modifySubjectButton = document.getElementById("button-modify");
    modifySubjectButton.disabled = event.target.value.length < 1
}

document.getElementById("subject-id-input").addEventListener('input', onInput)
