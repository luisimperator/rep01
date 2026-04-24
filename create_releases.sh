#!/bin/bash
# Script para criar GitHub Releases para HeavyDrops Transcoder
#
# COMO USAR:
# 1. Gere um token em: https://github.com/settings/tokens (com permissão 'repo')
# 2. Execute: GH_TOKEN=seu_token ./create_releases.sh
#    ou: gh auth login (e depois ./create_releases.sh)

set -e

REPO="luisimperator/rep01"

echo "=== Criando GitHub Releases para HeavyDrops Transcoder ==="
echo ""

# v1.2
echo "[1/7] Criando release v1.2..."
gh release create v1.2 HeavyDrops_Transcoder_v1.2.zip \
  --repo "$REPO" \
  --title "HeavyDrops Transcoder v1.2" \
  --notes "## HeavyDrops Transcoder v1.2

Initial release of Dropbox Video Transcoder.

### Features
- Hardware Acceleration (Intel QSV, NVIDIA NVENC, CPU fallback)
- File Stability Detection
- HEVC Detection (auto-skip)
- Metadata Preservation
- Idempotent Processing

### Requirements
- Python 3.11+
- FFmpeg with HEVC encoder support
- Dropbox API access token"

# v1.2.1
echo "[2/7] Criando release v1.2.1..."
gh release create v1.2.1 HeavyDrops_Transcoder_v1.2.1_Installer.zip \
  --repo "$REPO" \
  --title "HeavyDrops Transcoder v1.2.1" \
  --notes "## HeavyDrops Transcoder v1.2.1

### Changes
- Bug fixes and stability improvements
- Improved installer experience"

# v1.2.2
echo "[3/7] Criando release v1.2.2..."
gh release create v1.2.2 HeavyDrops_Transcoder_v1.2.2_Installer.zip \
  --repo "$REPO" \
  --title "HeavyDrops Transcoder v1.2.2" \
  --notes "## HeavyDrops Transcoder v1.2.2

### Changes
- Bug fixes and stability improvements"

# v1.2.3
echo "[4/7] Criando release v1.2.3..."
gh release create v1.2.3 HeavyDrops_Transcoder_v1.2.3_Installer.zip \
  --repo "$REPO" \
  --title "HeavyDrops Transcoder v1.2.3" \
  --notes "## HeavyDrops Transcoder v1.2.3

### Changes
- Bug fixes and stability improvements"

# v1.2.4
echo "[5/7] Criando release v1.2.4..."
gh release create v1.2.4 HeavyDrops_Transcoder_v1.2.4_Installer.zip \
  --repo "$REPO" \
  --title "HeavyDrops Transcoder v1.2.4" \
  --notes "## HeavyDrops Transcoder v1.2.4

### Changes
- Bug fixes and stability improvements"

# v1.2.5
echo "[6/7] Criando release v1.2.5..."
gh release create v1.2.5 HeavyDrops_Transcoder_v1.2.5_Installer.zip \
  --repo "$REPO" \
  --title "HeavyDrops Transcoder v1.2.5" \
  --notes "## HeavyDrops Transcoder v1.2.5

### Changes
- Bug fixes and stability improvements"

# v1.2.6
echo "[7/7] Criando release v1.2.6..."
gh release create v1.2.6 HeavyDrops_Transcoder_v1.2.6_Installer.zip \
  --repo "$REPO" \
  --title "HeavyDrops Transcoder v1.2.6" \
  --notes "## HeavyDrops Transcoder v1.2.6

### Changes
- Bug fixes and stability improvements
- Latest stable release"

echo ""
echo "=== Todas as releases foram criadas! ==="
echo ""
echo "Links de download estáveis:"
echo "  v1.2:   https://github.com/$REPO/releases/download/v1.2/HeavyDrops_Transcoder_v1.2.zip"
echo "  v1.2.1: https://github.com/$REPO/releases/download/v1.2.1/HeavyDrops_Transcoder_v1.2.1_Installer.zip"
echo "  v1.2.2: https://github.com/$REPO/releases/download/v1.2.2/HeavyDrops_Transcoder_v1.2.2_Installer.zip"
echo "  v1.2.3: https://github.com/$REPO/releases/download/v1.2.3/HeavyDrops_Transcoder_v1.2.3_Installer.zip"
echo "  v1.2.4: https://github.com/$REPO/releases/download/v1.2.4/HeavyDrops_Transcoder_v1.2.4_Installer.zip"
echo "  v1.2.5: https://github.com/$REPO/releases/download/v1.2.5/HeavyDrops_Transcoder_v1.2.5_Installer.zip"
echo "  v1.2.6: https://github.com/$REPO/releases/download/v1.2.6/HeavyDrops_Transcoder_v1.2.6_Installer.zip"
