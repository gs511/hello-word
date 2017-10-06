from flask import Flask,render_template,request

app= Flask(__name__)

@app.route("/")
def main():
	return "welcome to calculator. type the operation you want to perform after/ eg: /add/number"

@app.route("/<int:number>")
def numb(number):
	return "square of your number is %i" %number^3


if __name__ == '__main__':
	app.run(debug=True)
	
# ff
### dsfms,d	