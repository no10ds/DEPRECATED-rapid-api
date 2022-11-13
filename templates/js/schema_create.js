let schema = {};
let step = 0;
let key_value_tags = {};
let key_tags = [];

handle_step();

function generate_schema() {
  if (isValidForm("create_form")) {
    handle_generation();
  } else {
    // TODO
  }
}

function upload_schema() {
  if (isValidForm("validate_form")) {
    handle_upload();
  } else {
    // TODO
  }
}

function handle_upload() {
  const ownerName = document.getElementById("owner_name").value;
  const ownerEmail = document.getElementById("owner_email").value;
  const updateBehaviour = document.getElementById("select_behaviour").value;
  const spinner = document.getElementsByClassName("loading-spinner")[1];

  const owners = [{ email: ownerEmail, name: ownerName }];
  schema.metadata.owners = owners;
  schema.metadata.key_only_tags = key_tags;
  schema.metadata.key_value_tags = key_value_tags;
  schema.metadata.update_behaviour = updateBehaviour;

  console.log("SCHEMA UPLOAD", schema);
  spinner.style.display = "block";
}

function handle_generation() {
  const sensitivity = document.getElementById("select_sensitivity").value;
  const domain = document.getElementById("domain").value;
  const title = document.getElementById("title").value;
  const file = document.getElementById("file").files[0];
  const spinner = document.getElementsByClassName("loading-spinner")[0];

  if (sensitivity && domain && title && file) {
    spinner.style.display = "block";

    let data = new FormData();
    data.append("file", file);

    fetch(`${sensitivity}/${domain}/${title}/generate`, {
      method: "POST",
      body: data,
    })
      .then((response) =>
        response.json().then((result) => {
          spinner.style.display = "none";

          schema = result;
          step = 1;
          handle_step();
          populate_table(result);
        })
      )
      .catch(() => {
        spinner.style.display = "none";
        console.log("ERROR");
        // TODO
      });
  }
}

function populate_table(schema) {
  const table = document.getElementById("schema_table");
  schema.columns.forEach((col) => {
    const { data_type, allow_null, name } = col;
    const row = table.insertRow();
    row.innerHTML = `
        <td>${name}</td>
        <td>
            <select class="form-select_dropdown_table" selected="${data_type}" data-name="${name}" data-type="data_type">
            <option ${data_type === "Int64" ? "selected" : ""}>Int64</option>
            <option ${
              data_type === "Float64" ? "selected" : ""
            }>Float64</option> 
            <option ${data_type === "object" ? "selected" : ""}>object</option> 
            <option ${data_type === "date" ? "selected" : ""}>date</option> 
            <option ${
              data_type === "boolean" ? "selected" : ""
            }>boolean</option> 
            </select>
        </td>
        <td>
            <select class="form-select_dropdown_table selected="${allow_null}" data-name="${name}" data-type="allow_null">
            <option ${allow_null === true ? "selected" : ""}>true</option>
            <option ${allow_null === false ? "selected" : ""}>false</option>
            </select>
        </td>
    `;
  });

  const selects = document.getElementsByClassName("form-select_dropdown_table");
  for (let i = 0; i < selects.length; i++) {
    selects[i].addEventListener("change", onDataTypeChange);
  }
}

function updateSchema(columnName, value, updateType) {
  const index = schema.columns.findIndex((col) => col.name === columnName);
  if (updateType === "data_type") {
    schema.columns[index].data_type = value;
  } else {
    schema.columns[index].allow_null = value;
  }
  console.log(schema);
}

function onDataTypeChange(elem) {
  const target = elem.target;
  const value = target.value;
  const name = target.dataset.name;
  const updateType = target.dataset.type;
  return updateSchema(name, value, updateType);
}

function renderKeyValueTags() {
  const tagArea = document.getElementById("key_value_tags");
  while (tagArea.firstChild.id !== "key_value_false_tag") {
    tagArea.removeChild(tagArea.firstChild);
  }

  Object.keys(key_value_tags).forEach((key) => {
    const value = key_value_tags[key];
    const tagRow = document.createElement("div");
    tagRow.className = "key-value-input--row";
    tagRow.innerHTML = `
      <input class="form-input_text" type="text" value="${key}" disabled>
      <input class="form-input_text" type="text" value="${value}" disabled>
      `;
    tagArea.insertBefore(
      tagRow,
      document.getElementById("key_value_false_tag")
    );
  });
}

function renderKeyTags() {
  const tagArea = document.getElementById("key_tags");
  while (tagArea.firstChild.id !== "key_false_tag") {
    tagArea.removeChild(tagArea.firstChild);
  }

  key_tags.forEach((key) => {
    const tagRow = document.createElement("div");
    tagRow.className = "key-input--row";
    tagRow.innerHTML = `
      <input class="form-input_text" type="text" value="${key}" disabled>
    `;
    tagArea.insertBefore(tagRow, document.getElementById("key_false_tag"));
  });
}

function createKeyValueTag() {
  const keyElem = document.getElementById("key_value_false_tag_key");
  const valueElem = document.getElementById("key_value_false_tag_value");
  const key = keyElem.value;
  const value = valueElem.value;
  if (key && value) {
    key_value_tags[key] = value;
    renderKeyValueTags();
    keyElem.value = "";
    valueElem.value = "";
  }
}

function createKeyTag() {
  const keyElem = document.getElementById("key_false_tag_key");
  const key = keyElem.value;
  if (key) {
    key_tags.push(key);
    renderKeyTags();
    keyElem.value = "";
  }
}

function handle_step() {
  if (step === 0) {
    document.getElementById("form_step_0").style.visibility = "visible";
    document.getElementById("form_step_1").style.visibility = "hidden";

    document.getElementById("file").onchange = () => {
      document.getElementById("filename").textContent =
        document.getElementById("file").files[0].name;
    };
  } else if (step === 1) {
    document.getElementById("form_step_0").style.visibility = "hidden";
    document.getElementById("form_step_1").style.visibility = "visible";
    renderKeyValueTags();
    renderKeyTags();
  }
}
