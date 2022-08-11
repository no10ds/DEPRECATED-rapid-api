document.getElementById("select_subject").onchange = () => {
  if(document.getElementById("select_subject").value == "CLIENT") {
    document.getElementById("email_input").style.display = "none";
    document.getElementById("email").disabled = true;
  } else {
    document.getElementById("email_input").style.display = "grid";
    document.getElementById("email").disabled = false;
  }
}
