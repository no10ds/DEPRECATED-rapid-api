var sessionTimer = function () {
    const FIVE_MINUTES = 300000;
    let time;
    window.onload = resetTimer;
    // DOM Events
    document.onmousemove = resetTimer;
    document.onmousedown = resetTimer;
    document.ontouchstart = resetTimer;
    document.onclick = resetTimer;
    document.onkeydown = resetTimer;

    function logout() {
        window.location.href = `${location.protocol}//${location.host}/logout`
    }

    function resetTimer() {
        clearTimeout(time);
        time = setTimeout(logout, FIVE_MINUTES)
    }
};

window.onload = function() {
  sessionTimer();
}
