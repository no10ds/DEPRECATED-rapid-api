var sessionTimer = function () {
    // 300000 milliseconds = 5 minutes
    const TIME_OUT_IN_MS = 300000;
    let time;
    window.onload = resetTimer;
    // DOM Events
    document.onmousemove = resetTimer;
    document.onmousedown = resetTimer;
    document.ontouchstart = resetTimer;
    document.onclick = resetTimer;
    document.onkeydown = resetTimer;

    function logout() {
        window.location.href = "logout"
    }

    function resetTimer() {
        clearTimeout(time);
        time = setTimeout(logout, TIME_OUT_IN_MS)
    }
};

window.onload = function() {
  sessionTimer();
}
