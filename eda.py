import pandas as pd

# matches_df = pd.read_csv('data/matches_summarized.csv')
weapons_df = pd.read_csv('data/weapons_summarized.csv')

def matches_eda(df):
    row_num=df.shape[0]
    col_num=df.shape[1]
    nan_num=df.isna().sum()
    col_names=df.columns
    num_matches=df['matchId'].nunique()
    num_playes=df['player'].nunique()
    cat_features=df[['player','matchId','mapId','modeId','modeKey','modeName','modeImageUrl','modeMaxRounds','isAvailable','timestamp','metadataResult','map','mapName','mapImageUrl','seasonName','userId',
    'hasWon','result','agentName','playtimeDisplayType','roundsPlayedDisplayType','roundsWonDisplayType','roundsLostDisplayType','roundsDisconnectedDisplayType','placementDisplayType','scoreDisplayType',
    'killsDisplayType','deathsDisplayType','assistsDisplayType','damageDisplayType','damageReceivedDisplayType','headshotsDisplayType','grenadeCastsDisplayType','ability1CastsDisplayType','ability2CastsDisplayType',
    'ultimateCastsDisplayType','dealtHeadshotsDisplayType','dealtBodyshotsDisplayType','dealtLegshotsDisplayType','econRatingDisplayType','suicidesDisplayType','revivedDisplayType','firstBloodsDisplayType','firstDeathsDisplayType',
    'lastDeathsDisplayType','survivedDisplayType','tradedDisplayType','kastedDisplayType','kASTDisplayType','flawlessDisplayType','thriftyDisplayType','acesDisplayType','teamAcesDisplayType','clutchesDisplayType','clutchesLostDisplayType',
    'plantsDisplayType','defusesDisplayType','kdRatioDisplayType','scorePerRoundDisplayType','damagePerRoundDisplayType','headshotsPercentageDisplayType','rankDisplayType']]
    #só dá uma olhada nesse aqui, pq eu n lembrava direito qual o operator pra pegar todas as colunas exceto as que tão escolhidas
    num_features=df[~['player','matchId','mapId','modeId','modeKey','modeName','modeImageUrl','modeMaxRounds','isAvailable','timestamp','metadataResult','map','mapName','mapImageUrl','seasonName','userId',
    'hasWon','result','agentName','playtimeDisplayType','roundsPlayedDisplayType','roundsWonDisplayType','roundsLostDisplayType','roundsDisconnectedDisplayType','placementDisplayType','scoreDisplayType',
    'killsDisplayType','deathsDisplayType','assistsDisplayType','damageDisplayType','damageReceivedDisplayType','headshotsDisplayType','grenadeCastsDisplayType','ability1CastsDisplayType','ability2CastsDisplayType',
    'ultimateCastsDisplayType','dealtHeadshotsDisplayType','dealtBodyshotsDisplayType','dealtLegshotsDisplayType','econRatingDisplayType','suicidesDisplayType','revivedDisplayType','firstBloodsDisplayType','firstDeathsDisplayType',
    'lastDeathsDisplayType','survivedDisplayType','tradedDisplayType','kastedDisplayType','kASTDisplayType','flawlessDisplayType','thriftyDisplayType','acesDisplayType','teamAcesDisplayType','clutchesDisplayType','clutchesLostDisplayType',
    'plantsDisplayType','defusesDisplayType','kdRatioDisplayType','scorePerRoundDisplayType','damagePerRoundDisplayType','headshotsPercentageDisplayType','rankDisplayType']]


def weapons_eda(df):
    row_num=df.shape[0]
    col_num=df.shape[1]
    nan_num=df.isna().sum()
    col_names=df.columns
    total_weapons=df['weaponName'].unique()
    kills_per_weapon_per_player=df.groupby('player')[['player','weaponName','killsValue']]
    headshot_percentage_per_weapon_per_player=df.groupby('player')[['player','weaponName','headshotsPercentageDisplayValue']]
    dmg_done_per_weapon_per_player=df.groupby('player')[['player','weaponName','damageValue']]
    avg_kill_per_round_per_player=df.groupby(['player'])[['player','killsPerRoundValue']].mean(numeric_only=True)
    longest_kill_distance_per_weapon_per_player=df.groupby('weaponName')[['player','weaponName','longestKillDistanceValue']]
    cat_features=df[['player','weaponName','matchesPlayedDisplayType', 'matchesWonDisplayType', 'matchesLostDisplayType',
    'matchesTiedDisplayType', 'matchesWinPctDisplayType', 'roundsPlayedDisplayType', 'killsDisplayType',
    'killsPerRoundDisplayType', 'killsPerMatchDisplayType', 'secondaryKillsDisplayType', 'headshotsDisplayType',
    'secondaryKillsPerRoundDisplayType', 'secondaryKillsPerMatchDisplayType', 'deathsDisplayType', 'deathsPerRoundDisplayType',
    'deathsPerMatchDisplayType', 'kDRatioDisplayType', 'headshotsPercentageDisplayType', 'damageDisplayType',
    'damagePerRoundDisplayType', 'damagePerMatchDisplayType', 'damageReceivedDisplayType', 'dealtHeadshotsDisplayType',
    'dealtBodyshotsDisplayType', 'dealtLegshotsDisplayType', 'killDistanceDisplayType', 'avgKillDistanceDisplayType',
    'longestKillDistanceDisplayType']]
    num_features=df[~['player','weaponName','matchesPlayedDisplayType', 'matchesWonDisplayType', 'matchesLostDisplayType',
    'matchesTiedDisplayType', 'matchesWinPctDisplayType', 'roundsPlayedDisplayType', 'killsDisplayType',
    'killsPerRoundDisplayType', 'killsPerMatchDisplayType', 'secondaryKillsDisplayType', 'headshotsDisplayType',
    'secondaryKillsPerRoundDisplayType', 'secondaryKillsPerMatchDisplayType', 'deathsDisplayType', 'deathsPerRoundDisplayType',
    'deathsPerMatchDisplayType', 'kDRatioDisplayType', 'headshotsPercentageDisplayType', 'damageDisplayType',
    'damagePerRoundDisplayType', 'damagePerMatchDisplayType', 'damageReceivedDisplayType', 'dealtHeadshotsDisplayType',
    'dealtBodyshotsDisplayType', 'dealtLegshotsDisplayType', 'killDistanceDisplayType', 'avgKillDistanceDisplayType',
    'longestKillDistanceDisplayType']]
    
print(weapons_df.columns)
for i in weapons_df.columns:
    print(i)
# print(matches_df['matchId'].nunique())