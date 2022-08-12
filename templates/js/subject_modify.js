const modify = () => {
    const radios = document.querySelectorAll('input[type="radio"]')
    const checkboxes = document.querySelectorAll('input[type="checkbox"]')

    const checkedRadios = Array.from(radios).filter(radio => radio.checked)
    const checkedBoxes = Array.from(checkboxes).filter(checkbox => checkbox.checked)

    const selections = checkedRadios.concat(checkedBoxes)

    const filteredSelections = selections.filter(selection => selection.value !== "NONE")

    filteredSelections.forEach(element => {
        console.log(element.value)
    })
}
