import pandas as pd

matches_df = pd.read_csv('data/matches_summarized.csv')
weapons_df = pd.read_csv('data/weapons_summarized.csv')

def matches_eda(df):
    df.drop_duplicates('matchId',inplace=True)
    row_num=df.shape[0]
    col_num=df.shape[1]
    nan_num=df.isna().sum()
    col_names=df.columns
    num_matches=df['matchId'].nunique()
    num_playes=df['player'].nunique()
    cat_features = ['player','matchId','mapId','modeId','modeKey','modeName','modeImageUrl','modeMaxRounds','isAvailable','timestamp','metadataResult','map','mapName','mapImageUrl','seasonName','userId',
    'hasWon','result','agentName','playtimeDisplayType','roundsPlayedDisplayType','roundsWonDisplayType','roundsLostDisplayType','roundsDisconnectedDisplayType','placementDisplayType','scoreDisplayType',
    'killsDisplayType','deathsDisplayType','assistsDisplayType','damageDisplayType','damageReceivedDisplayType','headshotsDisplayType','grenadeCastsDisplayType','ability1CastsDisplayType','ability2CastsDisplayType',
    'ultimateCastsDisplayType','dealtHeadshotsDisplayType','dealtBodyshotsDisplayType','dealtLegshotsDisplayType','econRatingDisplayType','suicidesDisplayType','revivedDisplayType','firstBloodsDisplayType','firstDeathsDisplayType',
    'lastDeathsDisplayType','survivedDisplayType','tradedDisplayType','kastedDisplayType','kASTDisplayType','flawlessDisplayType','thriftyDisplayType','acesDisplayType','teamAcesDisplayType','clutchesDisplayType','clutchesLostDisplayType',
    'plantsDisplayType','defusesDisplayType','kdRatioDisplayType','scorePerRoundDisplayType','damagePerRoundDisplayType','headshotsPercentageDisplayType','rankDisplayType']
    cat_features_df=df[cat_features]
    num_features=matches_df.loc[:, ~matches_df.columns.isin(cat_features)]
    wins_per_agent = df.groupby(['agentName'],as_index=False)[['agentName','hasWon']].sum(numeric_only=True)
    highest_win_agent = wins_per_agent[wins_per_agent.hasWon == wins_per_agent.hasWon.max()]

def agents_eda(df):
    agents_stats=df.groupby('agentName',as_index=False)[['playtimeValue']].sum()
    agents_stats['playtimeHours']=agents_stats['playtimeValue']/3600
    win_percentage=100.0*df.groupby('agentName')[['hasWon']].sum()/df.groupby('agentName')[['hasWon']].count()
    agents_stats=agents_stats.join(win_percentage,on='agentName')
    kd_ratio=df.groupby('agentName')[['kdRatioValue']].sum()/df.groupby('agentName')[['kdRatioValue']].count()
    agents_stats=agents_stats.join(kd_ratio,on='agentName')
    adr=df.groupby('agentName')[['damagePerRoundValue']].sum()/df.groupby('agentName')[['damagePerRoundValue']].count()
    agents_stats=agents_stats.join(adr,on='agentName')
    acr=df.groupby('agentName')[['scorePerRoundValue']].sum()/df.groupby('agentName')[['scorePerRoundValue']].count()
    agents_stats=agents_stats.join(acr,on='agentName')
    hs_percentage=df.groupby('agentName')[['headshotsPercentageValue']].sum()/df.groupby('agentName')[['headshotsPercentageValue']].count()
    agents_stats=agents_stats.join(hs_percentage,on='agentName')
    kast=df.groupby('agentName')[['kASTValue']].sum()/df.groupby('agentName')[['kASTValue']].count()
    agents_stats=agents_stats.join(kast,on='agentName')
    return agents_stats

def players_eda(df):
    player_stats=df.groupby('player',as_index=False)[['playtimeValue']].sum()
    player_stats['playtimeHours']=player_stats['playtimeValue']/3600
    win_percentage=100.0*df.groupby('player')[['hasWon']].sum()/df.groupby('player')[['hasWon']].count()
    player_stats=player_stats.join(win_percentage,on='player')
    kd_ratio=df.groupby('player')[['kdRatioValue']].sum()/df.groupby('player')[['kdRatioValue']].count()
    player_stats=player_stats.join(kd_ratio,on='player')
    adr=df.groupby('player')[['damagePerRoundValue']].sum()/df.groupby('player')[['damagePerRoundValue']].count()
    player_stats=player_stats.join(adr,on='player')
    acr=df.groupby('player')[['scorePerRoundValue']].sum()/df.groupby('player')[['scorePerRoundValue']].count()
    player_stats=player_stats.join(acr,on='player')
    hs_percentage=df.groupby('player')[['headshotsPercentageValue']].sum()/df.groupby('player')[['headshotsPercentageValue']].count()
    player_stats=player_stats.join(hs_percentage,on='player')
    kast=df.groupby('player')[['kASTValue']].sum()/df.groupby('player')[['kASTValue']].count()
    player_stats=player_stats.join(kast,on='player')
    return player_stats

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
    cat_features=['player','weaponName','matchesPlayedDisplayType', 'matchesWonDisplayType', 'matchesLostDisplayType',
    'matchesTiedDisplayType', 'matchesWinPctDisplayType', 'roundsPlayedDisplayType', 'killsDisplayType',
    'killsPerRoundDisplayType', 'killsPerMatchDisplayType', 'secondaryKillsDisplayType', 'headshotsDisplayType',
    'secondaryKillsPerRoundDisplayType', 'secondaryKillsPerMatchDisplayType', 'deathsDisplayType', 'deathsPerRoundDisplayType',
    'deathsPerMatchDisplayType', 'kDRatioDisplayType', 'headshotsPercentageDisplayType', 'damageDisplayType',
    'damagePerRoundDisplayType', 'damagePerMatchDisplayType', 'damageReceivedDisplayType', 'dealtHeadshotsDisplayType',
    'dealtBodyshotsDisplayType', 'dealtLegshotsDisplayType', 'killDistanceDisplayType', 'avgKillDistanceDisplayType',
    'longestKillDistanceDisplayType']
    cat_features_df=df[cat_features]
    num_features=matches_df.loc[:, ~matches_df.columns.isin(cat_features)]

def weapon_usage_percentage(df, weapon_name):
    weapon_used = df.groupby('weaponName')[['matchesPlayedValue']].sum().loc[[weapon_name]]['matchesPlayedValue']
    return 100.0 * weapon_used/df['matchesPlayedValue'].sum()
