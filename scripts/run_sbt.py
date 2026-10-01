"""Lanza sbt desde make de forma no interactiva.

1. Varios sbt a la vez sobre el mismo proyecto (p.ej. `make 01_raw-uploader`
   y `make 02_parquet-converter` en dos terminales): el segundo choca con el
   lock de arranque del primero ("Could not create lock for
   \\\\.\\pipe\\sbt-load..._lock") y pregunta "Create a new server? y/n".
   Con stdin desde /dev/null (sin terminal) y -Dsbt.server.forcestart=true,
   sbt arranca sin ese servidor en vez de preguntar o salir.

2. Solo Windows: `sbt` es `sbt.bat`, y al pulsar Ctrl+C cmd.exe pregunta
   "¿Desea terminar el trabajo por lotes (S/N)?". make termina antes de que
   se conteste, así que PowerShell y ese cmd huérfano se pelean por el teclado
   (la "S" acaba en PowerShell: "El término 's' no se reconoce...").
   Este wrapper ignora Ctrl+C (lo recibe también el java de sbt, que se para
   limpiamente con sus shutdown hooks), espera a que java termine y mata el
   cmd antes de que pueda leer la respuesta.

Uso: python scripts/run_sbt.py <argumentos de sbt>
"""
import os
import shutil
import signal
import subprocess
import sys
import time


def _processes():
    """(pid, pid del padre, ejecutable) de todos los procesos (Toolhelp32)."""
    import ctypes
    from ctypes import wintypes

    class PROCESSENTRY32(ctypes.Structure):
        _fields_ = [
            ("dwSize", wintypes.DWORD),
            ("cntUsage", wintypes.DWORD),
            ("th32ProcessID", wintypes.DWORD),
            ("th32DefaultHeapID", ctypes.c_void_p),
            ("th32ModuleID", wintypes.DWORD),
            ("cntThreads", wintypes.DWORD),
            ("th32ParentProcessID", wintypes.DWORD),
            ("pcPriClassBase", ctypes.c_long),
            ("dwFlags", wintypes.DWORD),
            ("szExeFile", ctypes.c_char * 260),
        ]

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.CreateToolhelp32Snapshot.restype = wintypes.HANDLE
    snapshot = kernel32.CreateToolhelp32Snapshot(0x2, 0)  # TH32CS_SNAPPROCESS
    entry = PROCESSENTRY32()
    entry.dwSize = ctypes.sizeof(PROCESSENTRY32)
    procs = []
    try:
        ok = kernel32.Process32First(snapshot, ctypes.byref(entry))
        while ok:
            procs.append((entry.th32ProcessID, entry.th32ParentProcessID,
                          entry.szExeFile.decode(errors="replace").lower()))
            ok = kernel32.Process32Next(snapshot, ctypes.byref(entry))
    finally:
        kernel32.CloseHandle(snapshot)
    return procs


def _only_cmd_left(root_pid):
    """True si en el árbol de `root_pid` ya solo quedan cmd.exe (esperando el S/N)."""
    procs = _processes()
    pending, tree = [root_pid], []
    while pending:
        parent = pending.pop()
        for pid, ppid, exe in procs:
            if ppid == parent and pid != parent:
                tree.append(exe)
                pending.append(pid)
    return all(exe in ("cmd.exe", "conhost.exe") for exe in tree)


def main(args):
    args = ["-Dsbt.server.forcestart=true", *args]

    if os.name != "nt":
        os.dup2(os.open(os.devnull, os.O_RDONLY), 0)
        os.execvp("sbt", ["sbt", *args])

    sbt = shutil.which("sbt")
    if sbt is None:
        sys.exit("sbt no está en el PATH")

    interrupted = False

    def on_interrupt(signum, frame):
        nonlocal interrupted
        interrupted = True

    signal.signal(signal.SIGINT, on_interrupt)
    signal.signal(signal.SIGBREAK, on_interrupt)

    proc = subprocess.Popen([sbt, *args], stdin=subprocess.DEVNULL)
    while proc.poll() is None:
        # Tras Ctrl+C, cuando java (nieto: cmd de sbt.bat -> java) ya ha
        # terminado, los cmd solo quedan para preguntar (S/N): se matan (con
        # los .bat anidados, p.ej. shims de scoop) sin esperar respuesta.
        if interrupted and _only_cmd_left(proc.pid):
            subprocess.run(["taskkill", "/F", "/T", "/PID", str(proc.pid)],
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            proc.wait()
            break
        time.sleep(0.1)

    return 130 if interrupted else proc.returncode


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
