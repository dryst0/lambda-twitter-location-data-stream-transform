from abc import ABC
import csv

from math import sin

from TwitterPredictors import happyfuntokenizing

# Based from an academic paper called
# Developing Age and Gender Predictive Lexica over Social Media
# A paper by Maarten Sap, Gregory Park, Johannes C. Eichstaedt,
# Margaret L. Kern, David Stillwell, Michal Kosinski, Lyle H. Ungar
# and H. Andrew Schwartz
# Source: http://wwbp.org/papers/emnlp2014_developingLexica.pdf
class SapBasedPrediction(ABC):

	def __init__(self, lexica_file=""):
		self.lexica = self.load_lexica(lexica_file)
		self.intercept = float(self.lexica.pop('_intercept'))
		self.tokenizer = happyfuntokenizing.Tokenizer(preserve_case=False)

	def load_lexica(self, file_name):
		lexica = {}
		with open(file_name, mode='r') as infile:
			reader = csv.DictReader(infile)
			lexica = {record.get('term'):float(record.get('weight')) for record in reader}

		return lexica

	def get_token_frequencies(self, tokens):
		per_word_frequencies = {}

		for token in tokens:
			per_word_frequencies[token] = per_word_frequencies.get(token, 0) + 1

		return per_word_frequencies

	def get_feature_value(self, per_token_frequencies):
		lexicon_usage = 0
		total_tokens = sum([frequency for frequency in per_token_frequencies.values()])

		for token, frequency in per_token_frequencies.items():
			if token in self.lexica:
				lexical_weight_of_token = self.lexica[token]
				lexicon_usage +=  lexical_weight_of_token * (frequency / total_tokens)
		
		return lexicon_usage


class SapBasedAgePrediction(SapBasedPrediction):

	# This function returns a float, representing the age
	def predict_age(self, text):
		tokens = self.tokenizer.tokenize(text)
		feature_value = self.get_feature_value(self.get_token_frequencies(tokens))
		age = feature_value + self.intercept

		return age


class SapBasedGenderPrediction(SapBasedPrediction):

  def predict_gender(self,text):
    tokens = self.tokenizer.tokenize(text)
    feature_value = self.get_feature_value(self.get_token_frequencies(tokens))
    p = sin(feature_value + self.intercept)

    if p >= 0:
      return 'f'
    else: 
      return 'm'
