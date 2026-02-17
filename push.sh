#!/bin/bash

# Warna untuk output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${YELLOW}🚀 Memulai proses push ke repository...${NC}"

# Cek apakah ini repository git
if ! git rev-parse --git-dir > /dev/null 2>&1; then
    echo -e "${RED}❌ Bukan direktori git!${NC}"
    exit 1
fi

# Tampilkan status sebelum push
echo -e "\n${YELLOW}📊 Status repository saat ini:${NC}"
git status -s

# Add semua perubahan
echo -e "\n${YELLOW}📦 Menambahkan semua perubahan...${NC}"
git add .

# Cek apakah ada perubahan
if git diff --cached --quiet; then
    echo -e "${YELLOW}📭 Tidak ada perubahan untuk di-commit${NC}"
    
    # Tanya apakah tetap ingin push
    read -p "Tidak ada perubahan. Tetap push? (y/n): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 0
    fi
else
    # Commit dengan timestamp
    TIMESTAMP=$(date "+%Y-%m-%d %H:%M:%S")
    COMMIT_MSG="Bulk push - $TIMESTAMP"
    
    echo -e "\n${YELLOW}💾 Melakukan commit: ${COMMIT_MSG}${NC}"
    git commit -m "$COMMIT_MSG"
fi

# Push ke remote
echo -e "\n${YELLOW}📤 Push ke remote repository...${NC}"
if git push; then
    echo -e "${GREEN}✅ Push berhasil!${NC}"
    
    # Tampilkan informasi commit terakhir
    echo -e "\n${YELLOW}📋 Commit terakhir:${NC}"
    git log -1 --oneline
else
    echo -e "${RED}❌ Push gagal.${NC}"
    echo -e "\nKemungkinan penyebab:"
    echo "1. Remote repository tidak ditemukan"
    echo "2. Tidak ada koneksi internet"
    echo "3. Permission denied"
    echo "4. Remote branch berbeda dengan local"
    echo -e "\nCoba jalankan: git pull origin [branch-name] --rebase"
    exit 1
fi