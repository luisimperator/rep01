========================================
  HeavyDrops Transcoder - Instalador
========================================

COMO INSTALAR:
--------------
1. Clique duas vezes em "Install_HeavyDrops.bat"
2. Clique "Sim" quando pedir permissao de administrador
3. Aguarde o download do FFmpeg (~100MB)
4. Pronto! Atalho criado na Area de Trabalho

O QUE SERA INSTALADO:
---------------------
- FFmpeg: codificador de video (C:\Program Files\FFmpeg)
- HeavyDrops Transcoder (C:\Program Files\HeavyDrops Transcoder)
- Atalhos na Area de Trabalho e Menu Iniciar

REQUISITOS:
-----------
- Windows 10/11
- Python 3.8+ (se nao tiver, instale com: winget install Python.Python.3.12)
- Conexao com internet (para baixar FFmpeg)

APOS INSTALACAO:
----------------
- Reinicie o computador para garantir que o FFmpeg esta no PATH
- Abra o programa pelo atalho "HeavyDrops Transcoder"

PROBLEMAS?
----------
Se o FFmpeg nao for encontrado apos reiniciar:
1. Abra PowerShell como Administrador
2. Execute: winget install ffmpeg

Se Python nao for encontrado:
1. Abra PowerShell como Administrador
2. Execute: winget install Python.Python.3.12
