const modify = (subjectId) => {
    const radios = document.querySelectorAll('input[type="radio"]')
    const checkboxes = document.querySelectorAll('input[type="checkbox"]')

    const checkedRadios = Array.from(radios).filter(radio => radio.checked)
    const checkedBoxes = Array.from(checkboxes).filter(checkbox => checkbox.checked)

    const selections = checkedRadios.concat(checkedBoxes)

    const filteredSelections = selections.filter(selection => selection.value !== "NONE")

    const requestBody = {
        "subject_id": subjectId,
        "permissions": filteredSelections.map(element => element.value)
    }

    fetch("/client/permissions", {
        method: "PUT",
        // This is needed to treat this call as a browser request
        headers: new Headers({'Accept': 'text/html,application/json', 'Content-Type': 'application/json'}),
        body: JSON.stringify(requestBody)
    }).then(_ => {
        window.location.href = `${window.location.href}/success`
    })

}
