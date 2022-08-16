toggle_inputs("select_subject", "CLIENT", "email_input");

function create_subject() {
  const name = document.getElementById("name").value;
  const permissions = extract_selections();
  const subject_type = get_selected_value("select_subject");

  if (subject_type == "User") {
    const email = document.getElementById("email").value;
    create_user(name, email, permissions);
  } else if (subject_type == "Client") {
    create_client(name, permissions);
  } else {
    return null;
  }
}

function create_user(name, email, permissions) {
  const requestBody = {
    username: name,
    email: email,
    permissions: permissions.map((element) => element.value),
  };

  render_creation_success("user", requestBody);
}

function create_client(name, permissions) {
  const requestBody = {
    client_name: name,
    permissions: permissions.map((element) => element.value),
  };

  render_creation_success("client", requestBody);
}

function render_creation_success(type, requestBody) {
  fetch(`/${type}`, {
    method: "POST",
    // This is needed to treat this call as a browser request
    headers: new Headers({
      Accept: "text/html,application/json",
      "Content-Type": "application/json",
    }),
    body: JSON.stringify(requestBody),
  })
    .then((response) => {
      return response.json();
    })
    .then((data) => {
      window.location.href = `${window.location.href}/${
        data.user_id ? type == "user" : data.client_id
      }/success`;
    });
}
