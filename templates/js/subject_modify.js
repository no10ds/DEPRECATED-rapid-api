const modify = () => {
    const radios = document.querySelectorAll('input[type="radio"]')
    const checkboxes = document.querySelectorAll('input[type="checkbox"]')

    const checkedRadios = Array.from(radios).filter(radio => radio.checked)
    const checkedBoxes = Array.from(checkboxes).filter(radio => radio.checked)

    const selections = checkedRadios.concat(checkedBoxes)

    selections.forEach(element => {
        console.log(element.value)
    })
}
