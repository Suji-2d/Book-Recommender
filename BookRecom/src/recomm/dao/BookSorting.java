package recomm.dao;

// Comparable class in inherited
public class BookSorting implements Comparable<BookSorting> {

	// <book>, <likeability> and count variables are created
	private String book;
	private double pairCount;
	private double likeability;
	
	// compareTo method is overridden to sort the object by likeability score in descending order
	@Override
	public int compareTo(BookSorting o) {
		return Double.compare(o.likeability,likeability);
	}

	
	public BookSorting() {
		// TODO Auto-generated constructor stub
	}
	

	public BookSorting(String book,double pairCount, double likeability) {
		super();
		this.book = book;
		this.pairCount = pairCount;
		this.likeability = likeability;
	}


	public double getPairCount() {
		return pairCount;
	}


	public void setPairCount(double pairCount) {
		this.pairCount = pairCount;
	}


	public String getBook() {
		return book;
	}


	public void setBook(String book) {
		this.book = book;
	}


	public double getLikeability() {
		return likeability;
	}


	public void setLikeability(double likeability) {
		this.likeability = likeability;
	}


}
