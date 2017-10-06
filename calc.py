from flask import Flask,render_template,request

app= Flask(__name__)

@app.route("/")
def main():
	return "welcome to calculator. type the operation you want to perform after/ eg: /add/number"

@app.route("/<int:number>")
def numb(number):
	return "square of your number is {}".format(number)


if __name__ == '__main__':
	app.run()
	
# ff
### dsfms,d	