"""MTGJSON TypedDict sub-models (vendored from mtgjson5, stripped of doc metadata)."""

from __future__ import annotations

from typing import Required

from typing_extensions import TypedDict

# === Core Card Sub-Models ===


class ForeignDataIdentifiers(TypedDict, total=False):
    multiverseId: str
    scryfallId: str


class ForeignData(TypedDict, total=False):
    faceName: str
    flavorText: str
    identifiers: ForeignDataIdentifiers
    language: Required[str]
    multiverseId: int
    name: Required[str]
    text: str
    type: str
    uuid: str


class Identifiers(TypedDict, total=False):
    abuId: str
    cardtraderId: str
    csiId: str
    miniaturemarketId: str
    mvpId: str
    scgId: str
    tntId: str
    cardKingdomEtchedId: str
    cardKingdomFoilId: str
    cardKingdomId: str
    cardsphereId: str
    cardsphereFoilId: str
    deckboxId: str
    mcmId: str
    mcmMetaId: str
    mtgArenaId: str
    mtgjsonFoilVersionId: str
    mtgjsonNonFoilVersionId: str
    mtgjsonV4Id: str
    mtgoFoilId: str
    mtgoId: str
    multiverseId: str
    scryfallId: str
    scryfallCardBackId: str
    scryfallIllustrationId: str
    scryfallOracleId: str
    tcgplayerEtchedProductId: str
    tcgplayerProductId: str


class LeadershipSkills(TypedDict):
    brawl: bool
    commander: bool
    oathbreaker: bool


class Legalities(TypedDict, total=False):
    alchemy: str
    brawl: str
    commander: str
    duel: str
    explorer: str
    future: str
    gladiator: str
    historic: str
    historicbrawl: str
    legacy: str
    modern: str
    oathbreaker: str
    oldschool: str
    pauper: str
    paupercommander: str
    penny: str
    pioneer: str
    predh: str
    premodern: str
    standard: str
    standardbrawl: str
    timeless: str
    vintage: str


class PurchaseUrls(TypedDict, total=False):
    cardKingdom: str
    cardKingdomEtched: str
    cardKingdomFoil: str
    cardmarket: str
    tcgplayer: str
    tcgplayerEtched: str


class RelatedCards(TypedDict, total=False):
    reverseRelated: list[str]
    spellbook: list[str]
    tokens: list[str]


class Rulings(TypedDict):
    date: str
    text: str


class SourceProducts(TypedDict, total=False):
    etched: list[str]
    foil: list[str]
    nonfoil: list[str]


class TokenProductIdentifiers(TypedDict, total=False):
    tcgplayerProductId: str


class TokenProductPurchaseUrls(TypedDict, total=False):
    tcgplayer: str


class TokenProductPart(TypedDict, total=False):
    faceAttribute: list[str]
    uuid: Required[str]


class TokenProduct(TypedDict, total=False):
    identifiers: TokenProductIdentifiers
    purchaseUrls: TokenProductPurchaseUrls
    tokenParts: list[TokenProductPart]


# === Meta / Translations ===


class Meta(TypedDict):
    date: str
    version: str


class Translations(TypedDict, total=False):
    AncientGreek: str | None
    Arabic: str | None
    ChineseSimplified: str | None
    ChineseTraditional: str | None
    French: str | None
    German: str | None
    Hebrew: str | None
    Italian: str | None
    Japanese: str | None
    Korean: str | None
    Latin: str | None
    Phyrexian: str | None
    PortugueseBrazil: str | None
    Russian: str | None
    Sanskrit: str | None
    Spanish: str | None


class TcgplayerSkus(TypedDict, total=False):
    condition: Required[str]
    finish: str
    language: Required[str]
    printing: Required[str]
    productId: Required[int]
    skuId: Required[int]


# === Booster Configuration ===


class BoosterSheet(TypedDict, total=False):
    allowDuplicates: bool
    balanceColors: bool
    cards: Required[dict[str, int]]
    foil: Required[bool]
    fixed: bool
    totalWeight: Required[int]


class BoosterPack(TypedDict):
    contents: dict[str, int]
    weight: int


class BoosterConfig(TypedDict, total=False):
    boosters: Required[list[BoosterPack]]
    boostersTotalWeight: Required[int]
    name: str
    sheets: Required[dict[str, BoosterSheet]]
    sourceSetCodes: Required[list[str]]


# === Price Data ===


class PricePoints(TypedDict, total=False):
    etched: dict[str, float]
    foil: dict[str, float]
    normal: dict[str, float]


class PriceList(TypedDict, total=False):
    buylist: PricePoints
    currency: Required[str]
    retail: PricePoints


class PriceFormats(TypedDict, total=False):
    mtgo: dict[str, PriceList]
    paper: dict[str, PriceList]


# === Sealed Product Contents ===


class SealedProductCard(TypedDict, total=False):
    finishes: list[str]
    foil: bool
    name: Required[str]
    number: Required[str]
    set: Required[str]
    uuid: Required[str]


class SealedProductDeck(TypedDict):
    name: str
    set: str


class SealedProductOther(TypedDict):
    name: str


class SealedProductPack(TypedDict):
    code: str
    set: str


class SealedProductSealed(TypedDict, total=False):
    count: Required[int]
    name: Required[str]
    set: Required[str]
    uuid: str


class SealedProductVariableConfig(TypedDict, total=False):
    chance: int
    weight: int


class SealedProductVariableItem(TypedDict, total=False):
    card: list[SealedProductCard]
    deck: list[SealedProductDeck]
    other: list[SealedProductOther]
    pack: list[SealedProductPack]
    sealed: list[SealedProductSealed]
    variable_config: list[SealedProductVariableConfig]


class SealedProductVariableEntry(TypedDict, total=False):
    configs: list[SealedProductVariableItem]


class SealedProductContents(TypedDict, total=False):
    card: list[SealedProductCard]
    deck: list[SealedProductDeck]
    other: list[SealedProductOther]
    pack: list[SealedProductPack]
    sealed: list[SealedProductSealed]
    variable: list[SealedProductVariableEntry]


# === Compiled Data Structures ===


class Keywords(TypedDict):
    abilityWords: list[str]
    keywordAbilities: list[str]
    keywordActions: list[str]


class CardType(TypedDict):
    subTypes: list[str]
    superTypes: list[str]


class CardTypes(TypedDict):
    artifact: CardType
    battle: CardType
    conspiracy: CardType
    creature: CardType
    enchantment: CardType
    instant: CardType
    land: CardType
    phenomenon: CardType
    plane: CardType
    planeswalker: CardType
    scheme: CardType
    sorcery: CardType
    tribal: CardType
    vanguard: CardType
