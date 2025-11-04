from src.service.match_service import riot_data_pipeline


def main():
    print("Initializing application...")
    print("Application initialized.\n")

    # --- 매치 데이터 요청 테스트 ---
    riot_data_pipeline()


if __name__ == "__main__":
    main()
