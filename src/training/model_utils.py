import torch
import torch.nn as nn

class CategoricalEncoder(nn.Module):
    def __init__(self, input_dim, cat_indices, embedding_configs, out_channels):
        """
        Args:
            input_dim (int): 원본 Feature 차원 크기
            cat_indices (list): 임베딩할 컬럼의 인덱스 리스트 (예: [1, 2, 3])
            embedding_configs (dict): {col_idx: (num_embeddings, embed_dim)}
                                      JSON 로드 시 키가 "1" (str)일 수도 있고, 1 (int)일 수도 있음.
            out_channels (int): 최종 출력 차원
        """
        super().__init__()
        self.cat_indices = set(cat_indices)
        self.embeddings = nn.ModuleDict()
        
        total_concat_dim = 0
        
        # 1. Continuous Feature 개수 계산
        # 전체 차원 - 범주형 차원 개수
        num_continuous = input_dim - len(cat_indices)
        total_concat_dim += num_continuous
        
        # 2. Embedding Layer 생성
        for idx in cat_indices:
            # [수정 2] Key Type 호환성 처리 (Int vs String)
            # JSON에서 로드하면 키가 "1" (str)로 들어오지만, cat_indices는 [1] (int)일 수 있음
            if idx in embedding_configs:
                config_val = embedding_configs[idx]
            elif str(idx) in embedding_configs:
                config_val = embedding_configs[str(idx)]
            else:
                raise KeyError(f"Embedding config for index {idx} (or '{idx}') not found in configuration.")

            # JSON 리스트([num, dim])나 튜플((num, dim)) 모두 unpacking 가능
            num_emb, emb_dim = config_val
            
            # nn.ModuleDict는 키가 반드시 String이어야 함 (Pytorch 제약)
            self.embeddings[str(idx)] = nn.Embedding(int(num_emb), int(emb_dim), padding_idx=0)
            total_concat_dim += int(emb_dim)
            
        # 3. Final MLP
        self.mlp = nn.Sequential(
            nn.Linear(total_concat_dim, out_channels),
            nn.BatchNorm1d(out_channels),
            nn.ReLU()
        )

    def forward(self, x):
        # x shape: (Batch, input_dim)
        slices = []
        
        # Continuous Features 처리 (범주형 인덱스가 아닌 것들)
        cont_cols = [i for i in range(x.size(1)) if i not in self.cat_indices]
        
        if cont_cols:
            # 슬라이싱으로 연속형 변수 추출
            cont_x = x[:, cont_cols]
            slices.append(cont_x)
            
        # Categorical Features 처리 (임베딩)
        # nn.ModuleDict는 키가 str이므로 str(idx)로 접근
        for idx in sorted(list(self.cat_indices)):
            col_data = x[:, idx].long()
            emb_vec = self.embeddings[str(idx)](col_data)
            slices.append(emb_vec)
            
        # 이어 붙이기
        concat_x = torch.cat(slices, dim=1)
        
        return self.mlp(concat_x)